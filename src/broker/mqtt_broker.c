/**************************************************************************************************
 * @file        mqtt_broker.c
 * @brief       MQTT broker core: subscription matching and PUBLISH forwarding over TCP;
 *              protocol only, no business logic. Supports an MQTT 3.1.1/5.0 subset.
 *************************************************************************************************/
#include "mqtt_broker.h"
#include "event_loop.h"
#include "log.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MQTT_SERVER_IP "0.0.0.0"
#define MQTT_SERVER_PORT 1883
#define MQTT_QOS1_RETRY_MS 1000U
#define MQTT_QOS1_MAX_RETRIES 5U

/**************************************************************************************************
 * @brief       MQTT v3.1.1/v5.0 fixed packet type definitions
 *************************************************************************************************/
#define MQTT_TYPE_CONNECT 0x10
#define MQTT_TYPE_CONNACK 0x20
#define MQTT_TYPE_PUBLISH 0x30
#define MQTT_TYPE_SUBSCRIBE 0x82
#define MQTT_TYPE_SUBACK 0x90
#define MQTT_TYPE_UNSUBSCRIBE 0xA2
#define MQTT_TYPE_UNSUBACK 0xB0
#define MQTT_TYPE_PINGREQ 0xC0
#define MQTT_TYPE_PINGRESP 0xD0
#define MQTT_TYPE_DISCONNECT 0xE0
#define MQTT_TYPE_PUBACK 0x40

/**************************************************************************************************
 * @brief       Global MQTT server context
 *************************************************************************************************/
static MqttServerContext g_server = {0};
/** eventfd used to wake up the epoll event loop for TX queue processing */
static int g_tx_queue_event_fd = -1;
static int g_qos1_retry_timer_fd = -1;

/** Forward declaration of TX queue processing (used by the event callbacks below) */
static int server_send_from_tx_queue(void);

typedef enum
{
    SERVER_FRAME_LEN_OK = 0,
    SERVER_FRAME_LEN_DECODE_ERR,
    SERVER_FRAME_LEN_INCOMPLETE
} ServerFrameLenResult;

/**************************************************************************************************
 * @brief       Copy a NUL-terminated string with truncation into a fixed-size buffer
 *************************************************************************************************/
static void mqtt_copy_text(uint8_t *dst, size_t dst_size, const uint8_t *src)
{
    size_t copy_len = 0;

    if (!dst || dst_size == 0U)
    {
        return;
    }

    if (!src)
    {
        dst[0] = '\0';
        return;
    }

    copy_len = strlen((const char *)src);
    if (copy_len >= dst_size)
    {
        copy_len = dst_size - 1U;
    }

    memcpy(dst, src, copy_len);
    dst[copy_len] = '\0';
}

/**************************************************************************************************
 * @brief       Get the current monotonic clock in milliseconds
 *************************************************************************************************/
static uint64_t server_monotonic_ms(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    {
        return 0U;
    }
    return (uint64_t)ts.tv_sec * 1000U + (uint64_t)ts.tv_nsec / 1000000U;
}

/**************************************************************************************************
 * @brief       Set a socket to non-blocking mode
 * @param[in]   fd  Socket file descriptor
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
static int set_socket_non_blocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
    {
        return -1;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**************************************************************************************************
 * @brief       Send data non-blocking; if the socket write buffer is full, buffer it in the session pending_tx list
 * @param[in]   session Target client session
 * @param[in]   data    Data to send (copied into the pending buffer when needed)
 * @param[in]   len     Data length in bytes
 * @return      true when fully sent (or accepted into the pending list), false on send failure
 *
 * When send() returns EAGAIN, the remaining bytes are copied into session->pending_tx
 * and EPOLLOUT is enabled so that server_flush_pending_tx resumes once the socket is
 * writable again. No blocking select() wait is used, keeping the event loop responsive.
 *************************************************************************************************/
static bool socket_send_nonblock(ServerClientSession *session, const uint8_t *data, size_t len)
{
    int sock = session->socket_fd;
    size_t sent = 0;

    if (!session->is_connected || !data || len == 0U)
    {
        return false;
    }

    /*
     * While unsent MQTT frames are still queued in the TCP stream, a new frame must be
     * appended behind them; sending directly would interleave it before the leftover
     * bytes and break the peer's MQTT framing.
     */
    if (session->pending_tx_tail != NULL)
    {
        PendingTxNode *node = (PendingTxNode *)malloc(sizeof(PendingTxNode));
        if (node == NULL)
        {
            return false;
        }
        node->data = (uint8_t *)malloc(len);
        if (node->data == NULL)
        {
            free(node);
            return false;
        }
        memcpy(node->data, data, len);
        node->total_len = len;
        node->sent_offset = 0;
        node->next = NULL;
        session->pending_tx_tail->next = node;
        session->pending_tx_tail = node;
        (void)event_loop_mod_fd(sock, EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP);
        return true;
    }

    while (sent < len)
    {
        ssize_t ret = send(sock, data + sent, len - sent, MSG_NOSIGNAL);
        if (ret > 0)
        {
            sent += (size_t)ret;
            continue;
        }

        if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            /* Buffer the unsent remainder into the pending_tx list */
            size_t remaining = len - sent;
            PendingTxNode *node = (PendingTxNode *)malloc(sizeof(PendingTxNode));
            if (node == NULL)
            {
                return false;
            }
            node->data = (uint8_t *)malloc(remaining);
            if (node->data == NULL)
            {
                free(node);
                return false;
            }
            memcpy(node->data, data + sent, remaining);
            node->total_len = remaining;
            node->sent_offset = 0;
            node->next = NULL;

            if (session->pending_tx_tail == NULL)
            {
                session->pending_tx_head = node;
                session->pending_tx_tail = node;
            }
            else
            {
                session->pending_tx_tail->next = node;
                session->pending_tx_tail = node;
            }

            /* Enable EPOLLOUT to resume sending once writable; queued data counts as accepted */
            event_loop_mod_fd(sock, EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP);
            return true;
        }

        return false;
    }

    return true;
}

/**************************************************************************************************
 * @brief       Free the client's pending TX list
 * @param[in]   session Client session pointer
 *************************************************************************************************/
static void server_free_pending_tx(ServerClientSession *session)
{
    PendingTxNode *node = session->pending_tx_head;
    while (node != NULL)
    {
        PendingTxNode *next = node->next;
        if (node->data != NULL)
        {
            free(node->data);
        }
        free(node);
        node = next;
    }
    session->pending_tx_head = NULL;
    session->pending_tx_tail = NULL;
}

/**************************************************************************************************
 * @brief       Free the client's in-flight QoS1 retransmission list
 * @param[in]   session Client session pointer
 *************************************************************************************************/
static void server_free_qos1_inflight(ServerClientSession *session)
{
    PendingQos1Node *node = session->qos1_inflight;

    while (node != NULL)
    {
        PendingQos1Node *next = node->next;
        free(node->packet);
        free(node);
        node = next;
    }
    session->qos1_inflight = NULL;
}

/**************************************************************************************************
 * @brief       Allocate the next outgoing packet identifier (wraps 0xFFFF -> 1)
 * @param[in]   session Client session pointer
 * @return      Next packet identifier
 *************************************************************************************************/
static uint16_t server_next_packet_id(ServerClientSession *session)
{
    session->next_out_packet_id++;
    if (session->next_out_packet_id == 0U)
    {
        session->next_out_packet_id = 1U;
    }
    return session->next_out_packet_id;
}

/**************************************************************************************************
 * @brief       Track a sent QoS1 packet until its PUBACK arrives
 * @param[in]   session   Client session pointer
 * @param[in]   packet_id Packet identifier
 * @param[in]   packet    Wire packet to keep for retransmission
 * @param[in]   packet_len Packet length in bytes
 * @return      true on success, false on allocation failure
 *************************************************************************************************/
static bool server_track_qos1(ServerClientSession *session, uint16_t packet_id,
                              const uint8_t *packet, size_t packet_len)
{
    PendingQos1Node *node = (PendingQos1Node *)calloc(1U, sizeof(PendingQos1Node));

    if (node == NULL)
    {
        return false;
    }
    node->packet = (uint8_t *)malloc(packet_len);
    if (node->packet == NULL)
    {
        free(node);
        return false;
    }
    memcpy(node->packet, packet, packet_len);
    node->packet_id = packet_id;
    node->packet_len = packet_len;
    node->last_send_ms = server_monotonic_ms();
    node->next = session->qos1_inflight;
    session->qos1_inflight = node;
    return true;
}

/**************************************************************************************************
 * @brief       Handle a received PUBACK and drop the matching in-flight QoS1 entry
 * @param[in]   session   Client session pointer
 * @param[in]   packet_id Acknowledged packet identifier
 *************************************************************************************************/
static void server_handle_puback(ServerClientSession *session, uint16_t packet_id)
{
    PendingQos1Node **link = &session->qos1_inflight;

    while (*link != NULL)
    {
        PendingQos1Node *node = *link;
        if (node->packet_id == packet_id)
        {
            *link = node->next;
            free(node->packet);
            free(node);
            log_info("mqtt PUBACK from %s packet_id=%u", session->client_id, packet_id);
            return;
        }
        link = &node->next;
    }
    log_warn("mqtt unexpected PUBACK from %s packet_id=%u", session->client_id, packet_id);
}

/**************************************************************************************************
 * @brief       Try to flush the session pending TX list (invoked on EPOLLOUT)
 * @param[in]   session Client session pointer
 * @return      true if everything was flushed, false if data is still pending
 *************************************************************************************************/
static bool server_flush_pending_tx(ServerClientSession *session)
{
    while (session->pending_tx_head != NULL)
    {
        PendingTxNode *node = session->pending_tx_head;
        size_t remaining = node->total_len - node->sent_offset;

        while (remaining > 0)
        {
            ssize_t ret = send(session->socket_fd,
                               node->data + node->sent_offset,
                               remaining, MSG_NOSIGNAL);
            if (ret > 0)
            {
                node->sent_offset += (size_t)ret;
                remaining -= (size_t)ret;
                continue;
            }

            if (ret < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                /* Still not writable; keep EPOLLOUT and wait for the next trigger */
                return false;
            }

            /* Send error; drop the whole pending list */
            log_warn("mqtt pending tx failed: client=%s errno=%d",
                     session->client_id, errno);
            server_free_pending_tx(session);
            return true;
        }

        session->pending_tx_head = node->next;
        if (session->pending_tx_head == NULL)
        {
            session->pending_tx_tail = NULL;
        }
        if (node->data != NULL)
        {
            free(node->data);
        }
        free(node);
    }

    /* Everything flushed; drop EPOLLOUT and keep EPOLLIN only */
    event_loop_mod_fd(session->socket_fd, EPOLLIN | EPOLLET | EPOLLRDHUP);
    return true;
}

/**************************************************************************************************
 * @brief       Reset the RX buffer back to the 2048-byte inline buffer (frees heap growth)
 *************************************************************************************************/
static void server_rx_buf_reset(ServerClientSession *session)
{
    if (!session)
    {
        return;
    }
    if (session->rx_buf != NULL && session->rx_buf != session->rx_inline)
    {
        free(session->rx_buf);
    }
    session->rx_buf = session->rx_inline;
    session->rx_cap = MQTT_SERVER_RX_INLINE_SIZE;
    session->rx_len = 0U;
}

/**************************************************************************************************
 * @brief       Compute a new buffer capacity by doubling until it satisfies the requirement
 * @param[in]   current_cap Current buffer capacity
 * @param[in]   need        Minimum required capacity
 * @return      Computed capacity, or 0 if it would exceed the maximum limit
 *************************************************************************************************/
static size_t calculate_rx_capacity(size_t current_cap, size_t need)
{
    size_t new_cap = (current_cap < MQTT_SERVER_RX_INLINE_SIZE) ? 
                      MQTT_SERVER_RX_INLINE_SIZE : current_cap;

    while (new_cap < need)
    {
        if (new_cap > (MQTT_SERVER_RX_MAX_SIZE / 2U))
        {
            new_cap = MQTT_SERVER_RX_MAX_SIZE;
            break;
        }
        new_cap *= 2U;
    }
    if (new_cap < need)
    {
        new_cap = need;
    }
    return (new_cap <= MQTT_SERVER_RX_MAX_SIZE) ? new_cap : 0U;
}

/**************************************************************************************************
 * @brief       Allocate the new buffer and migrate existing data (inline-to-heap or heap growth)
 * @param[in]   session Client session pointer
 * @param[in]   new_cap Target capacity in bytes
 * @return      true on success, false on allocation failure
 *************************************************************************************************/
static bool reallocate_rx_buffer(ServerClientSession *session, size_t new_cap)
{
    uint8_t *neu = NULL;

    if (session->rx_buf == session->rx_inline)
    {
        neu = (uint8_t *)malloc(new_cap);
        if (neu == NULL)
        {
            log_err("mqtt rx heap alloc failed need=%u", (unsigned int)new_cap);
            return false;
        }

        if (session->rx_len > 0U)
        {
            memcpy(neu, session->rx_inline, session->rx_len);
        }
    }
    else
    {
        neu = (uint8_t *)realloc(session->rx_buf, new_cap);
        if (neu == NULL)
        {
            log_err("mqtt rx heap realloc failed need=%u", (unsigned int)new_cap);
            return false;
        }
    }

    session->rx_buf = neu;
    session->rx_cap = new_cap;

    log_info("mqtt rx buffer grown client=%s cap=%u",
             session->client_id, (unsigned int)session->rx_cap);
    return true;
}

/**************************************************************************************************
 * @brief       Ensure the RX buffer has enough capacity, growing it when necessary
 * @param[in]   session Client session pointer
 * @param[in]   need    Minimum required capacity in bytes
 * @return      true on success, false on bad args, limit exceeded or allocation failure
 *************************************************************************************************/
static bool server_rx_buf_ensure(ServerClientSession *session, size_t need)
{
    if (!session || need == 0U)
    {
        return false;
    }

    if (session->rx_buf == NULL)
    {
        session->rx_buf = session->rx_inline;
        session->rx_cap = MQTT_SERVER_RX_INLINE_SIZE;
    }

    if (need <= session->rx_cap)
    {
        return true;
    }

    if (need > MQTT_SERVER_RX_MAX_SIZE)
    {
        log_warn("mqtt rx need %u exceeds max %u",
                 (unsigned int)need, (unsigned int)MQTT_SERVER_RX_MAX_SIZE);
        return false;
    }

    size_t new_cap = calculate_rx_capacity(session->rx_cap, need);
    if (new_cap == 0U)
    {
        return false;
    }

    return reallocate_rx_buffer(session, new_cap);
}

/**************************************************************************************************
 * @brief       Peek the total length of a complete frame from a received prefix (true once the remaining-length field is complete)
 *************************************************************************************************/
static bool server_mqtt_peek_frame_need(const uint8_t *pkt, ssize_t avail, ssize_t *need_out)
{
    uint32_t remaining_len = 0U;
    uint32_t multiplier = 1U;
    int rl_bytes = 0;

    if (!pkt || !need_out || avail < 2)
    {
        return false;
    }
    for (int i = 1; i <= 4; ++i)
    {
        uint8_t digit;
        if (i >= avail)
        {
            return false;
        }
        digit = pkt[i];
        remaining_len += (uint32_t)(digit & 0x7FU) * multiplier;
        if ((digit & 0x80U) == 0U)
        {
            rl_bytes = i;
            break;
        }
        multiplier *= 128U;
    }
    if (rl_bytes == 0)
    {
        return false;
    }
    *need_out = 1 + rl_bytes + (ssize_t)remaining_len;
    return (*need_out > 0);
}

/**************************************************************************************************
 * @brief       Reset a client session to its initial state
 * @param[in]   session Client session pointer
 *************************************************************************************************/
static void server_reset_client(ServerClientSession *session)
{
    if (!session)
    {
        return;
    }
    if (session->socket_fd >= 0)
    {
        close(session->socket_fd);
    }
    session->socket_fd = -1;
    session->is_connected = false;
    session->protocol_level = 4U;
    session->subscription_count = 0U;
    server_rx_buf_reset(session);
    memset(session->subscriptions, 0, sizeof(session->subscriptions));
    memset(session->subscription_qos, 0, sizeof(session->subscription_qos));
    memset(session->client_id, 0, sizeof(session->client_id));
    server_free_pending_tx(session);
    server_free_qos1_inflight(session);
    session->next_out_packet_id = 0U;
}

/**************************************************************************************************
 * @brief       Send an MQTT CONNACK
 * @param[in]   session         Client session
 * @param[in]   protocol_level  MQTT protocol version
 *************************************************************************************************/
static void mqtt_send_connack(ServerClientSession *session, uint8_t protocol_level)
{
    (void)session;
    if (protocol_level == 5U)
    {
        uint8_t pkt[] = {
            MQTT_TYPE_CONNACK, 0x03,
            0x00,
            0x00,
            0x00
        };
        socket_send_nonblock(session, pkt, sizeof(pkt));
    }
    else
    {
        uint8_t pkt[] = {MQTT_TYPE_CONNACK, 0x02, 0x00, 0x00};
        socket_send_nonblock(session, pkt, sizeof(pkt));
    }
}

/**************************************************************************************************
 * @brief       Send an MQTT PINGRESP
 * @param[in]   session  Client session
 *************************************************************************************************/
static void mqtt_send_pingresp(ServerClientSession *session)
{
    uint8_t pkt[] = {MQTT_TYPE_PINGRESP, 0x00};
    socket_send_nonblock(session, pkt, sizeof(pkt));
}

/**************************************************************************************************
 * @brief       Send an MQTT SUBACK
 * @param[in]   session         Client session
 * @param[in]   msg_id          Packet identifier
 * @param[in]   protocol_level  MQTT protocol version
 *************************************************************************************************/
static void mqtt_send_suback(ServerClientSession *session, uint16_t msg_id,
                             uint8_t protocol_level, uint8_t granted_qos)
{
    if (protocol_level == 5U)
    {
        uint8_t pkt[] = {
            MQTT_TYPE_SUBACK, 0x04,
            (uint8_t)(msg_id >> 8), (uint8_t)(msg_id & 0xFF),
            0x00,
            granted_qos
        };
        socket_send_nonblock(session, pkt, sizeof(pkt));
    }
    else
    {
        uint8_t pkt[] = {
            MQTT_TYPE_SUBACK, 0x03,
            (uint8_t)(msg_id >> 8), (uint8_t)(msg_id & 0xFF),
            granted_qos};
        socket_send_nonblock(session, pkt, sizeof(pkt));
    }
}

/**************************************************************************************************
 * @brief       Send an MQTT PUBACK
 * @param[in]   session  Client session
 * @param[in]   msg_id   Packet identifier
 *************************************************************************************************/
static void mqtt_send_puback(ServerClientSession *session, uint16_t msg_id)
{
    uint8_t pkt[] = {MQTT_TYPE_PUBACK, 0x02, (uint8_t)(msg_id >> 8), (uint8_t)(msg_id & 0xFF)};
    socket_send_nonblock(session, pkt, sizeof(pkt));
}

/**************************************************************************************************
 * @brief       Send an MQTT UNSUBACK
 * @param[in]   session         Client session
 * @param[in]   msg_id          Packet identifier
 * @param[in]   protocol_level  MQTT protocol version
 *************************************************************************************************/
static void mqtt_send_unsuback(ServerClientSession *session, uint16_t msg_id, uint8_t protocol_level)
{
    if (protocol_level == 5U)
    {
        uint8_t pkt[] = {
            MQTT_TYPE_UNSUBACK, 0x03,
            (uint8_t)(msg_id >> 8), (uint8_t)(msg_id & 0xFF),
            0x00
        };
        socket_send_nonblock(session, pkt, sizeof(pkt));
    }
    else
    {
        uint8_t pkt[] = {
            MQTT_TYPE_UNSUBACK, 0x02,
            (uint8_t)(msg_id >> 8), (uint8_t)(msg_id & 0xFF)};
        socket_send_nonblock(session, pkt, sizeof(pkt));
    }
}

/**************************************************************************************************
 * @brief       Decode the MQTT remaining-length field
 *************************************************************************************************/
static bool mqtt_decode_remaining_length(const uint8_t *buf, ssize_t read_len, int *rl_bytes, uint32_t *remaining_len)
{
    uint32_t value = 0;
    uint32_t multiplier = 1;
    int i = 0;

    if (!buf || !rl_bytes || !remaining_len || read_len < 2)
    {
        return false;
    }

    for (i = 1; i < 5 && i < read_len; ++i)
    {
        value += (uint32_t)(buf[i] & 0x7F) * multiplier;
        if ((buf[i] & 0x80) == 0)
        {
            *rl_bytes = i;
            *remaining_len = value;
            return true;
        }
        multiplier *= 128;
    }
    return false;
}

/**************************************************************************************************
 * @brief       Decode an MQTT variable-length integer (varint)
 *************************************************************************************************/
static bool mqtt_decode_varint(const uint8_t *buf, ssize_t read_len, int *used_bytes, uint32_t *value)
{
    uint32_t result = 0;
    uint32_t multiplier = 1;
    int i;

    if (!buf || !used_bytes || !value || read_len <= 0)
    {
        return false;
    }

    for (i = 0; i < 4 && i < read_len; ++i)
    {
        result += (uint32_t)(buf[i] & 0x7F) * multiplier;
        if ((buf[i] & 0x80) == 0)
        {
            *used_bytes = i + 1;
            *value = result;
            return true;
        }
        multiplier *= 128U;
    }
    return false;
}

/**************************************************************************************************
 * @brief       Skip the MQTT v5 PUBLISH Properties section
 *************************************************************************************************/
static bool mqtt_publish_skip_v5_properties(const uint8_t *buf, ssize_t read_len, int *pos_io)
{
    int prop_len_bytes = 0;
    uint32_t prop_len = 0;
    const int pos = *pos_io;

    if (!mqtt_decode_varint(buf + pos, read_len - pos, &prop_len_bytes, &prop_len))
    {
        return false;
    }
    *pos_io = pos + prop_len_bytes;
    if (*pos_io + (int)prop_len > read_len)
    {
        return false;
    }
    *pos_io += (int)prop_len;
    return true;
}

/**************************************************************************************************
 * @brief       PUBLISH: after the topic bytes, parse QoS and packet ID, advancing the cursor
 *************************************************************************************************/
static bool mqtt_publish_advance_qos_pktid(const uint8_t *buf,
                                           ssize_t read_len,
                                           int *pos_io,
                                           uint16_t *packet_id,
                                           uint8_t *qos)
{
    int pos = *pos_io;
    uint8_t msg_qos;

    msg_qos = (uint8_t)((buf[0] >> 1) & 0x03);
    if (msg_qos > 0U)
    {
        if (pos + 2 > read_len)
        {
            return false;
        }
        *packet_id = (uint16_t)((buf[pos] << 8) | buf[pos + 1]);
        pos += 2;
    }
    else
    {
        *packet_id = 0U;
    }

    *pos_io = pos;
    *qos = msg_qos;
    return true;
}

/**************************************************************************************************
 * @brief       PUBLISH: parse topic name (with optional wire copy), QoS and packet ID
 *************************************************************************************************/
static bool mqtt_publish_parse_topic_qos_pktid(const uint8_t *buf,
                                               ssize_t read_len,
                                               int *pos_io,
                                               uint16_t *packet_id,
                                               uint8_t *qos,
                                               uint8_t *wire_topic_out,
                                               size_t wire_topic_out_sz)
{
    int pos = *pos_io;
    uint16_t topic_len = 0;

    topic_len = (uint16_t)((buf[pos] << 8) | buf[pos + 1]);
    pos += 2;
    if (pos + topic_len > read_len)
    {
        return false;
    }
    if (wire_topic_out && wire_topic_out_sz > 0U)
    {
        size_t copy = (size_t)topic_len;
        if (copy >= wire_topic_out_sz)
        {
            copy = wire_topic_out_sz - 1U;
        }
        if (copy > 0U)
        {
            memcpy(wire_topic_out, buf + pos, copy);
        }
        wire_topic_out[copy] = '\0';
    }
    pos += topic_len;

    *pos_io = pos;
    return mqtt_publish_advance_qos_pktid(buf, read_len, pos_io, packet_id, qos);
}

/**************************************************************************************************
 * @brief       Locate the PUBLISH payload offset and length
 *************************************************************************************************/
static bool mqtt_get_publish_payload_offset(const uint8_t *buf,
                                            ssize_t read_len,
                                            int *payload_pos,
                                            uint32_t *payload_len,
                                            uint16_t *packet_id,
                                            uint8_t *qos,
                                            uint8_t protocol_level,
                                            uint8_t *wire_topic_out,
                                            size_t wire_topic_out_sz)
{
    int rl_bytes = 0;
    uint32_t remaining_len = 0;
    int pos = 0;
    int header_end = 0;
    uint8_t msg_qos = 0;

    if (!buf || !payload_pos || !payload_len || !packet_id || !qos)
    {
        return false;
    }

    if (!mqtt_decode_remaining_length(buf, read_len, &rl_bytes, &remaining_len))
    {
        return false;
    }

    header_end = 1 + rl_bytes;
    if ((uint32_t)(read_len - header_end) < remaining_len || header_end + 2 > read_len)
    {
        return false;
    }

    pos = header_end;
    if (!mqtt_publish_parse_topic_qos_pktid(buf, read_len, &pos, packet_id, &msg_qos, wire_topic_out, wire_topic_out_sz))
    {
        return false;
    }

    if (protocol_level == 5U)
    {
        if (!mqtt_publish_skip_v5_properties(buf, read_len, &pos))
        {
            return false;
        }
    }

    if (pos > read_len)
    {
        return false;
    }

    *payload_pos = pos;
    *payload_len = remaining_len - (uint32_t)(pos - header_end);
    *qos = msg_qos;
    return true;
}

/**************************************************************************************************
 * @brief       Get the protocol version from a CONNECT packet
 *************************************************************************************************/
static uint8_t mqtt_get_connect_protocol_level(const uint8_t *buf, ssize_t read_len)
{
    int rl_bytes = 0;
    uint32_t remaining_len = 0;
    int pos = 0;
    uint16_t proto_name_len = 0;

    if (!buf || read_len < 8)
    {
        return 4U;
    }

    if (!mqtt_decode_remaining_length(buf, read_len, &rl_bytes, &remaining_len))
    {
        return 4U;
    }

    pos = 1 + rl_bytes;
    if ((uint32_t)(read_len - pos) < remaining_len || pos + 2 >= read_len)
    {
        return 4U;
    }

    proto_name_len = (uint16_t)((buf[pos] << 8) | buf[pos + 1]);
    pos += 2;
    if (pos + proto_name_len >= read_len)
    {
        return 4U;
    }

    pos += proto_name_len;
    return buf[pos];
}

/**************************************************************************************************
 * @brief       Get the packet identifier from a SUBSCRIBE packet
 *************************************************************************************************/
static uint16_t mqtt_get_subscribe_packet_id(const uint8_t *buf, ssize_t read_len)
{
    int rl_bytes = 0;
    uint32_t remaining_len = 0;
    int pos = 0;

    if (!mqtt_decode_remaining_length(buf, read_len, &rl_bytes, &remaining_len))
    {
        return 0U;
    }

    pos = 1 + rl_bytes;
    if ((uint32_t)(read_len - pos) < remaining_len || pos + 1 >= read_len)
    {
        return 0U;
    }

    return (uint16_t)((buf[pos] << 8) | buf[pos + 1]);
}

/**************************************************************************************************
 * @brief       Get the packet identifier from an UNSUBSCRIBE packet
 *************************************************************************************************/
static uint16_t mqtt_get_unsubscribe_packet_id(const uint8_t *buf, ssize_t read_len)
{
    return mqtt_get_subscribe_packet_id(buf, read_len);
}

/**************************************************************************************************
 * @brief       SUBSCRIBE/UNSUBSCRIBE: locate the start/end offsets of the topic filter list
 *************************************************************************************************/
static bool mqtt_sub_unsub_payload_window(const uint8_t *buf,
                                          ssize_t read_len,
                                          uint8_t protocol_level,
                                          int *pos_out,
                                          int *end_pos_out)
{
    int rl_bytes = 0;
    uint32_t remaining_len = 0;
    int pos = 0;
    int end_pos = 0;

    if (!mqtt_decode_remaining_length(buf, read_len, &rl_bytes, &remaining_len))
    {
        return false;
    }

    pos = 1 + rl_bytes + 2;
    end_pos = (1 + rl_bytes) + (int)remaining_len;
    if (end_pos > read_len)
    {
        end_pos = (int)read_len;
    }

    if (protocol_level == 5U)
    {
        int prop_len_bytes = 0;
        uint32_t prop_len = 0;

        if (!mqtt_decode_varint(buf + pos, read_len - pos, &prop_len_bytes, &prop_len))
        {
            return false;
        }
        pos += prop_len_bytes + (int)prop_len;
    }

    *pos_out = pos;
    *end_pos_out = end_pos;
    return true;
}

/**************************************************************************************************
 * @brief       Log the topics of a SUBSCRIBE packet
 *************************************************************************************************/
static void mqtt_log_subscribe_topics(const uint8_t *buf, ssize_t read_len, uint8_t protocol_level, const uint8_t *client_id)
{
    int pos = 0;
    int end_pos = 0;

    if (!buf || !client_id)
    {
        return;
    }

    if (!mqtt_sub_unsub_payload_window(buf, read_len, protocol_level, &pos, &end_pos))
    {
        return;
    }

    while (pos + 2 <= end_pos)
    {
        uint16_t topic_len = (uint16_t)((buf[pos] << 8) | buf[pos + 1]);
        uint8_t topic[128] = {0};
        uint16_t copy_len;

        pos += 2;
        if (pos + topic_len >= end_pos)
        {
            return;
        }

        copy_len = topic_len >= sizeof(topic) ? (uint16_t)(sizeof(topic) - 1U) : topic_len;
        memcpy(topic, buf + pos, copy_len);
        log_info("mqtt rx SUBSCRIBE topic from %s: %s qos=%u",
                 client_id, topic, (unsigned int)(buf[pos + topic_len] & 0x03U));

        pos += topic_len + 1;
    }
}

/**************************************************************************************************
 * @brief       Log the topics of an UNSUBSCRIBE packet
 *************************************************************************************************/
static void mqtt_log_unsubscribe_topics(const uint8_t *buf, ssize_t read_len, uint8_t protocol_level, const uint8_t *client_id)
{
    int pos = 0;
    int end_pos = 0;

    if (!buf || !client_id)
    {
        return;
    }

    if (!mqtt_sub_unsub_payload_window(buf, read_len, protocol_level, &pos, &end_pos))
    {
        return;
    }

    while (pos + 2 <= end_pos)
    {
        uint16_t topic_len = (uint16_t)((buf[pos] << 8) | buf[pos + 1]);
        uint8_t topic[128] = {0};
        uint16_t copy_len;

        pos += 2;
        if (pos + topic_len > end_pos)
        {
            return;
        }

        copy_len = topic_len >= sizeof(topic) ? (uint16_t)(sizeof(topic) - 1U) : topic_len;
        memcpy(topic, buf + pos, copy_len);
        log_info("mqtt rx UNSUBSCRIBE topic from %s: %s", client_id, topic);
        pos += topic_len;
    }
}

/**************************************************************************************************
 * @brief       Add a subscription filter to a client session
 *************************************************************************************************/
static void mqtt_add_subscription(ServerClientSession *session, const uint8_t *topic_filter, uint8_t qos)
{
    if (!session || !topic_filter || topic_filter[0] == '\0')
    {
        return;
    }

    for (int i = 0; i < session->subscription_count; ++i)
    {
        if (strcmp((const char *)session->subscriptions[i], (const char *)topic_filter) == 0)
        {
            session->subscription_qos[i] = qos;
            return;
        }
    }

    if (session->subscription_count >= MQTT_SERVER_MAX_SUBSCRIPTIONS)
    {
        log_warn("mqtt subscription full for %s, ignore %s", session->client_id, topic_filter);
        return;
    }

    mqtt_copy_text(session->subscriptions[session->subscription_count],
                   MQTT_SERVER_TOPIC_LEN,
                   topic_filter);
    session->subscription_qos[session->subscription_count] = qos;
    session->subscription_count++;
}

/**************************************************************************************************
 * @brief       Remove a subscription filter from a client session
 *************************************************************************************************/
static void mqtt_remove_subscription(ServerClientSession *session, const uint8_t *topic_filter)
{
    if (!session || !topic_filter || topic_filter[0] == '\0')
    {
        return;
    }

    for (int i = 0; i < session->subscription_count; ++i)
    {
        if (strcmp((const char *)session->subscriptions[i], (const char *)topic_filter) == 0)
        {
            for (; i + 1 < session->subscription_count; ++i)
            {
                memcpy(session->subscriptions[i], session->subscriptions[i + 1], MQTT_SERVER_TOPIC_LEN);
                session->subscription_qos[i] = session->subscription_qos[i + 1];
            }
            memset(session->subscriptions[session->subscription_count - 1U], 0, MQTT_SERVER_TOPIC_LEN);
            session->subscription_qos[session->subscription_count - 1U] = 0U;
            session->subscription_count--;
            return;
        }
    }
}

/**************************************************************************************************
 * @brief       Match a topic against an MQTT filter ('+' and '#' wildcards)
 *************************************************************************************************/
static bool mqtt_topic_matches_filter(const uint8_t *filter, const uint8_t *topic)
{
    const uint8_t *filter_ptr = (const uint8_t *)filter;
    const uint8_t *topic_ptr = (const uint8_t *)topic;
    if (!filter_ptr || !topic_ptr)
    {
        return false;
    }
    while (*filter_ptr != '\0' && *topic_ptr != '\0')
    {
        if (*filter_ptr == '#')
        {
            return true;
        }

        if (*filter_ptr == '+')
        {
            while (*topic_ptr != '\0' && *topic_ptr != '/')
            {
                topic_ptr++;
            }

            filter_ptr++;

            if (*filter_ptr == '\0' && *topic_ptr == '\0')
            {
                return true;
            }

            if (*filter_ptr == '/' && *topic_ptr == '/')
            {
                filter_ptr++;
                topic_ptr++;
                continue;
            }

            return false;
        }

        if (*filter_ptr != *topic_ptr)
        {
            return false;
        }
        filter_ptr++;
        topic_ptr++;
    }
    return ((*filter_ptr == '#' && *(filter_ptr + 1) == '\0') || (*filter_ptr == '\0' && *topic_ptr == '\0'));
}

/**************************************************************************************************
 * @brief       Check whether a session has a subscription matching the topic
 *************************************************************************************************/
static bool mqtt_session_matches_topic(const ServerClientSession *session, const uint8_t *topic)
{
    if (!session || !topic || topic[0] == '\0')
    {
        return false;
    }

    for (int i = 0; i < session->subscription_count; ++i)
    {
        if (mqtt_topic_matches_filter(session->subscriptions[i], topic))
        {
            return true;
        }
    }
    return false;
}

/**************************************************************************************************
 * @brief       Get the highest granted QoS a session has for a topic
 * @param[in]   session Client session pointer
 * @param[in]   topic   Topic name
 * @return      Granted QoS, 0 if no subscription matches
 *************************************************************************************************/
static uint8_t mqtt_session_topic_qos(const ServerClientSession *session, const uint8_t *topic)
{
    if (!session || !topic)
    {
        return 0U;
    }
    for (int i = 0; i < session->subscription_count; ++i)
    {
        if (mqtt_topic_matches_filter(session->subscriptions[i], topic))
        {
            return session->subscription_qos[i];
        }
    }
    return 0U;
}

/**************************************************************************************************
 * @brief       Store the SUBSCRIBE topic filters and return the granted QoS
 *************************************************************************************************/
static uint8_t mqtt_store_subscribe_topics(ServerClientSession *session, const uint8_t *buf,
                                           ssize_t read_len, uint8_t protocol_level)
{
    int pos = 0;
    int end_pos = 0;
    uint8_t first_granted_qos = 0U;
    bool have_qos = false;

    if (!session || !buf)
    {
        return 0x80U;
    }
    if (!mqtt_sub_unsub_payload_window(buf, read_len, protocol_level, &pos, &end_pos))
    {
        return 0x80U;
    }

    while (pos + 2 <= end_pos)
    {
        uint16_t topic_len = (uint16_t)((buf[pos] << 8) | buf[pos + 1]);
        uint8_t topic[MQTT_SERVER_TOPIC_LEN] = {0};
        uint16_t copy_len;

        pos += 2;
        uint8_t requested_qos;

        if (pos + topic_len >= end_pos)
            return 0x80U;

        copy_len = topic_len >= sizeof(topic) ? (uint16_t)(sizeof(topic) - 1U) : topic_len;
        memcpy(topic, buf + pos, copy_len);
        requested_qos = (uint8_t)(buf[pos + topic_len] & 0x03U);
        if (requested_qos > 1U)
        {
            requested_qos = 1U;
        }
        mqtt_add_subscription(session, (const uint8_t *)topic, requested_qos);
        if (!have_qos)
        {
            first_granted_qos = requested_qos;
            have_qos = true;
        }
        pos += topic_len + 1;
    }
    return have_qos ? first_granted_qos : 0x80U;
}

/**************************************************************************************************
 * @brief       Remove the UNSUBSCRIBE topic filters from the session
 *************************************************************************************************/
static void mqtt_remove_unsubscribe_topics(ServerClientSession *session, const uint8_t *buf, ssize_t read_len, uint8_t protocol_level)
{
    int pos = 0;
    int end_pos = 0;

    if (!session || !buf)
    {
        return;
    }
    if (!mqtt_sub_unsub_payload_window(buf, read_len, protocol_level, &pos, &end_pos))
    {
        return;
    }

    while (pos + 2 <= end_pos)
    {
        uint16_t topic_len = (uint16_t)((buf[pos] << 8) | buf[pos + 1]);
        uint8_t topic[MQTT_SERVER_TOPIC_LEN] = {0};
        uint16_t copy_len;

        pos += 2;
        if (pos + topic_len > end_pos)
            return;

        copy_len = topic_len >= sizeof(topic) ? (uint16_t)(sizeof(topic) - 1U) : topic_len;
        memcpy(topic, buf + pos, copy_len);
        mqtt_remove_subscription(session, (const uint8_t *)topic);
        pos += topic_len;
    }
}

/**************************************************************************************************
 * @brief       Encode the MQTT remaining-length field
 *************************************************************************************************/
static int mqtt_encode_remaining_length(uint8_t *out, uint32_t len)
{
    int i = 0;
    do
    {
        uint8_t digit = (uint8_t)(len % 128U);
        len /= 128U;
        if (len > 0U)
            digit |= 0x80U;
        out[i++] = digit;
    } while (len > 0U && i < 4);
    return i;
}

/**************************************************************************************************
 * @brief       Build and send an MQTT PUBLISH (wire topic + raw payload, no JSON rewriting)
 * @note        Small packets (<= 2048 bytes) are built in a stack buffer to avoid malloc/free overhead.
 *************************************************************************************************/
#define MQTT_TX_STACK_BUF_SIZE 2048

static bool mqtt_send_publish_json(ServerClientSession *session, const uint8_t *publish_topic,
                                   const uint8_t *payload, uint32_t len,
                                   uint8_t protocol_level, uint8_t qos)
{
    bool send_ok;
    uint16_t packet_id = 0U;

    if (!publish_topic || publish_topic[0] == '\0')
    {
        log_warn("mqtt tx skip: empty topic");
        return false;
    }
    const uint8_t *topic = publish_topic;
    uint16_t topic_len = (uint16_t)strlen((const char *)topic);
    uint32_t remaining_len = 2U + topic_len + len + ((qos == 1U) ? 2U : 0U) +
                             ((protocol_level == 5U) ? 1U : 0U);
    uint8_t rl[4] = {0};
    int rl_len = mqtt_encode_remaining_length(rl, remaining_len);
    size_t packet_len = 1U + (size_t)rl_len + 2U + (size_t)topic_len + (size_t)len +
                        ((qos == 1U) ? 2U : 0U) + ((protocol_level == 5U) ? 1U : 0U);
    uint8_t stack_buf[MQTT_TX_STACK_BUF_SIZE];
    uint8_t *packet = stack_buf;
    bool heap_alloced = false;
    size_t pos = 0;

    if (packet_len > MQTT_TX_STACK_BUF_SIZE)
    {
        packet = (uint8_t *)malloc(packet_len);
        if (!packet)
        {
            log_err("mqtt publish alloc failed");
            return false;
        }
        heap_alloced = true;
    }

    if (qos == 1U)
    {
        packet_id = server_next_packet_id(session);
    }
    packet[pos++] = (uint8_t)(MQTT_TYPE_PUBLISH | ((qos == 1U) ? 0x02U : 0U));
    memcpy(packet + pos, rl, (size_t)rl_len);
    pos += (size_t)rl_len;
    packet[pos++] = (uint8_t)(topic_len >> 8);
    packet[pos++] = (uint8_t)(topic_len & 0xFFU);
    memcpy(packet + pos, topic, topic_len);
    pos += topic_len;

    if (qos == 1U)
    {
        packet[pos++] = (uint8_t)(packet_id >> 8);
        packet[pos++] = (uint8_t)(packet_id & 0xFFU);
    }
    if (protocol_level == 5U)
        packet[pos++] = 0x00;
    memcpy(packet + pos, payload, len);
    pos += len;
    if (pos != packet_len)
    {
        log_err("mqtt publish length mismatch: built=%u expected=%u qos=%u",
                (unsigned int)pos, (unsigned int)packet_len, qos);
        if (heap_alloced)
        {
            free(packet);
        }
        return false;
    }

    send_ok = socket_send_nonblock(session, packet, packet_len);
    if (!send_ok)
    {
        log_warn("mqtt send publish failed: client=%s topic=%s len=%u",
                 session->client_id, topic, len);
    }
    else if (qos == 1U)
    {
        if (!server_track_qos1(session, packet_id, packet, packet_len))
        {
            log_err("mqtt QoS1 track failed: client=%s topic=%s packet_id=%u",
                    session->client_id, topic, packet_id);
        }
        else
        {
            log_info("mqtt QoS1 sent: client=%s topic=%s packet_id=%u",
                     session->client_id, topic, packet_id);
        }
    }

    if (heap_alloced)
    {
        free(packet);
    }
    return send_ok;
}

/**************************************************************************************************
 * @brief       Create the TCP listener (standalone broker, fixed IPv4:1883)
 *************************************************************************************************/
static int server_socket_init(void)
{
    int listen_fd = 0;
    int opt = 1;
    struct sockaddr_in addr;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        log_err("external mqtt socket create failed");
        return -1;
    }

    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    if (set_socket_non_blocking(listen_fd) != 0)
    {
        close(listen_fd);
        return -1;
    }

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(MQTT_SERVER_IP);
    addr.sin_port = htons(MQTT_SERVER_PORT);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, MQTT_SERVER_MAX_CLIENTS) < 0)
    {
        close(listen_fd);
        return -1;
    }

    g_server.server_socket_fd = listen_fd;
    log_info("mqtt broker listening on %s:%d", MQTT_SERVER_IP, MQTT_SERVER_PORT);
    return 0;
}

/**************************************************************************************************
 * @brief       Compute the total length of a single MQTT frame (fixed header included)
 *************************************************************************************************/
static ServerFrameLenResult server_mqtt_calc_frame_len(const uint8_t *pkt, ssize_t pkt_read_len, ssize_t *frame_len_out)
{
    uint32_t remaining_len = 0;
    uint32_t multiplier = 1U;
    int rl_bytes = 0;
    ssize_t frame_len;

    if (!pkt || !frame_len_out || pkt_read_len < 2)
    {
        return SERVER_FRAME_LEN_INCOMPLETE;
    }
    for (int i = 1; i <= 4; ++i)
    {
        uint8_t digit;
        if (i >= pkt_read_len)
        {
            return SERVER_FRAME_LEN_INCOMPLETE;
        }
        digit = pkt[i];
        remaining_len += (uint32_t)(digit & 0x7FU) * multiplier;
        if ((digit & 0x80U) == 0U)
        {
            rl_bytes = i;
            break;
        }
        multiplier *= 128U;
    }
    if (rl_bytes == 0)
    {
        return SERVER_FRAME_LEN_DECODE_ERR;
    }
    frame_len = 1 + rl_bytes + (ssize_t)remaining_len;
    *frame_len_out = frame_len;
    if (frame_len <= 0 || frame_len > pkt_read_len)
    {
        return SERVER_FRAME_LEN_INCOMPLETE;
    }
    return SERVER_FRAME_LEN_OK;
}

/**************************************************************************************************
 * @brief       Handle one SUBSCRIBE frame
 *************************************************************************************************/
static void server_rx_dispatch_subscribe_frame(ServerClientSession *s, const uint8_t *pkt, ssize_t frame_len)
{
    uint8_t *cid = s->client_id;
    uint8_t prot = s->protocol_level;
    uint16_t msg_id = mqtt_get_subscribe_packet_id(pkt, frame_len);
    uint8_t granted_qos;

    mqtt_log_subscribe_topics(pkt, frame_len, prot, cid);
    granted_qos = mqtt_store_subscribe_topics(s, pkt, frame_len, prot);
    mqtt_send_suback(s, msg_id, prot, granted_qos);
    log_info("mqtt SUBACK to %s msg_id=%u granted_qos=%u", cid, msg_id, granted_qos);
}

/**************************************************************************************************
 * @brief       Handle one UNSUBSCRIBE frame
 *************************************************************************************************/
static void server_rx_dispatch_unsubscribe_frame(ServerClientSession *s, const uint8_t *pkt, ssize_t frame_len)
{
    uint8_t *cid = s->client_id;
    uint8_t prot = s->protocol_level;
    uint16_t msg_id = mqtt_get_unsubscribe_packet_id(pkt, frame_len);

    mqtt_send_unsuback(s, msg_id, prot);
    log_info("mqtt rx UNSUBSCRIBE from %s protocol=%u msg_id=%u -> UNSUBACK", cid, prot, msg_id);
    mqtt_log_unsubscribe_topics(pkt, frame_len, prot, cid);
    mqtt_remove_unsubscribe_topics(s, pkt, frame_len, prot);
}

/**************************************************************************************************
 * @brief       Handle one PUBLISH frame (enqueue and QoS1 PUBACK)
 *************************************************************************************************/
static void server_rx_handle_publish_frame(ServerClientSession *s, const uint8_t *pkt, ssize_t frame_len)
{
    int pos = 0;
    uint32_t payload_len = 0;
    uint16_t packet_id = 0;
    uint8_t qos = 0;
    uint8_t wire_topic[MQTT_SERVER_TOPIC_LEN] = {0};
    uint8_t prot = s->protocol_level;
    uint8_t *cid = s->client_id;

    if (!mqtt_get_publish_payload_offset(pkt, frame_len, &pos, &payload_len, &packet_id, &qos,
                                         prot, wire_topic, sizeof(wire_topic)))
    {
        log_warn("mqtt rx PUBLISH parse failed: client=%s protocol=%u len=%d", cid, prot, (int)frame_len);
        return;
    }
    if (pos >= frame_len || payload_len == 0U)
    {
        log_warn("mqtt rx PUBLISH empty payload: client=%s", cid);
        return;
    }
    /* Fan out by original wire topic; target_id carries sender for no-local echo. */
    {
        int fanout = 0;

        if (msg_enqueue(&g_server.tx_queue, cid, pkt + pos, payload_len, wire_topic) == 0)
        {
            fanout = server_send_from_tx_queue();
        }
        log_info("mqtt PUBLISH from %s topic=%s protocol=%u qos=%u payload_len=%u fanout=%d",
                 cid, wire_topic, prot, qos, payload_len, fanout);
    }

    if (qos == 1U)
    {
        mqtt_send_puback(s, packet_id);
    }
}

/**************************************************************************************************
 * @brief       Dispatch one MQTT frame; returns 0 to continue, non-zero ends the session (DISCONNECT)
 *************************************************************************************************/
static int server_rx_dispatch_one_frame(ServerClientSession *s, const uint8_t *pkt, ssize_t frame_len)
{
    uint8_t mqtt_type = (uint8_t)((pkt[0] >> 4) & 0x0F);
    uint8_t *cid = s->client_id;

    switch (mqtt_type)
    {
    case 0x01:
        s->protocol_level = mqtt_get_connect_protocol_level(pkt, frame_len);
        mqtt_send_connack(s, s->protocol_level);
        return 0;
    case 0x0C:
        mqtt_send_pingresp(s);
        return 0;
    case 0x08:
        server_rx_dispatch_subscribe_frame(s, pkt, frame_len);
        return 0;
    case 0x0A:
        server_rx_dispatch_unsubscribe_frame(s, pkt, frame_len);
        return 0;
    case 0x0E:
        server_reset_client(s);
        return 1;
    case 0x03:
        server_rx_handle_publish_frame(s, pkt, frame_len);
        return 0;
    case 0x04:
        if (frame_len >= 4)
        {
            uint16_t packet_id = (uint16_t)(((uint16_t)pkt[2] << 8) | pkt[3]);
            server_handle_puback(s, packet_id);
        }
        return 0;
    default:
        log_warn("mqtt rx unsupported packet type=0x%02X from %s protocol=%u len=%d", pkt[0], cid, s->protocol_level, (int)frame_len);
        return 0;
    }
}

/**************************************************************************************************
 * @brief       epoll callback: client socket readable, writable or closed
 *************************************************************************************************/
static void on_client_event(int fd, uint32_t events, void *userdata)
{
    ServerClientSession *session = (ServerClientSession *)userdata;

    (void)fd;

    if (!session || !session->is_connected)
    {
        return;
    }

    if ((events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0U)
    {
        log_info("client disconnected: %s (fd=%d)", session->client_id, session->socket_fd);
        event_loop_del_fd(session->socket_fd);
        server_reset_client(session);
        return;
    }

    /* EPOLLOUT: try to flush pending TX data */
    if ((events & EPOLLOUT) != 0U)
    {
        server_flush_pending_tx(session);
    }

    if ((events & EPOLLIN) != 0U)
    {
        while (1)
        {
            size_t space = 0U;
            ssize_t recv_len = 0;

            if (session->rx_buf == NULL)
            {
                session->rx_buf = session->rx_inline;
                session->rx_cap = MQTT_SERVER_RX_INLINE_SIZE;
            }

            /* With a known partial-frame length, grow the buffer to fit the whole frame first */
            if (session->rx_len > 0U)
            {
                ssize_t need = 0;
                if (server_mqtt_peek_frame_need(session->rx_buf, (ssize_t)session->rx_len, &need))
                {
                    if ((size_t)need > MQTT_SERVER_RX_MAX_SIZE)
                    {
                        log_warn("mqtt rx frame too large client=%s need=%d max=%u, reset",
                                 session->client_id, (int)need,
                                 (unsigned int)MQTT_SERVER_RX_MAX_SIZE);
                        event_loop_del_fd(session->socket_fd);
                        server_reset_client(session);
                        return;
                    }
                    if (!server_rx_buf_ensure(session, (size_t)need))
                    {
                        log_warn("mqtt rx grow failed client=%s need=%d, reset",
                                 session->client_id, (int)need);
                        event_loop_del_fd(session->socket_fd);
                        server_reset_client(session);
                        return;
                    }
                }
            }

            space = session->rx_cap - session->rx_len;
            if (space == 0U)
            {
                size_t want = session->rx_cap * 2U;
                if (want < session->rx_cap || want > MQTT_SERVER_RX_MAX_SIZE)
                {
                    want = MQTT_SERVER_RX_MAX_SIZE;
                }
                if (want <= session->rx_cap || !server_rx_buf_ensure(session, want))
                {
                    log_warn("mqtt rx buffer full client=%s len=%u cap=%u, reset session",
                             session->client_id, (unsigned int)session->rx_len,
                             (unsigned int)session->rx_cap);
                    event_loop_del_fd(session->socket_fd);
                    server_reset_client(session);
                    return;
                }
                space = session->rx_cap - session->rx_len;
            }

            recv_len = recv(session->socket_fd, session->rx_buf + session->rx_len, space, MSG_DONTWAIT);
            if (recv_len > 0)
            {
                size_t offset = 0U;

                session->rx_len += (size_t)recv_len;
                while (offset < session->rx_len)
                {
                    ssize_t frame_len = 0;
                    ServerFrameLenResult frame_ret =
                        server_mqtt_calc_frame_len(session->rx_buf + offset,
                                                   (ssize_t)(session->rx_len - offset),
                                                   &frame_len);

                    if (frame_ret == SERVER_FRAME_LEN_INCOMPLETE)
                    {
                        ssize_t need = 0;
                        if (server_mqtt_peek_frame_need(session->rx_buf + offset,
                                                        (ssize_t)(session->rx_len - offset),
                                                        &need))
                        {
                            size_t total_need = offset + (size_t)need;
                            if (total_need > MQTT_SERVER_RX_MAX_SIZE ||
                                !server_rx_buf_ensure(session, total_need))
                            {
                                log_warn("mqtt rx incomplete grow failed client=%s need=%u, reset",
                                         session->client_id, (unsigned int)total_need);
                                event_loop_del_fd(session->socket_fd);
                                server_reset_client(session);
                                return;
                            }
                        }
                        break;
                    }
                    if (frame_ret == SERVER_FRAME_LEN_DECODE_ERR)
                    {
                        log_warn("mqtt rx frame decode err client=%s, drop buffered %u bytes",
                                 session->client_id, (unsigned int)session->rx_len);
                        server_rx_buf_reset(session);
                        goto read_done;
                    }

                    if (server_rx_dispatch_one_frame(session, session->rx_buf + offset, frame_len) != 0)
                    {
                        return;
                    }
                    offset += (size_t)frame_len;
                }
                if (offset > 0U)
                {
                    size_t remain = session->rx_len - offset;
                    if (remain > 0U)
                    {
                        memmove(session->rx_buf, session->rx_buf + offset, remain);
                    }
                    session->rx_len = remain;
                    /* Return the heap buffer to the inline 2048 bytes when idle */
                    if (remain == 0U && session->rx_buf != session->rx_inline)
                    {
                        free(session->rx_buf);
                        session->rx_buf = session->rx_inline;
                        session->rx_cap = MQTT_SERVER_RX_INLINE_SIZE;
                    }
                }
            }
            else if (recv_len == 0)
            {
                log_info("client disconnected: %s (fd=%d)", session->client_id, session->socket_fd);
                event_loop_del_fd(session->socket_fd);
                server_reset_client(session);
                return;
            }
            else
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    break;
                }
                log_info("client read error: %s (fd=%d)", session->client_id, session->socket_fd);
                event_loop_del_fd(session->socket_fd);
                server_reset_client(session);
                return;
            }
        }
    read_done:;
    }
}

/**************************************************************************************************
 * @brief       epoll callback: new incoming connection on the listening socket
 *************************************************************************************************/
static void on_accept_event(int fd, uint32_t events, void *userdata)
{
    (void)fd;
    (void)userdata;

    if ((events & EPOLLIN) == 0U)
    {
        return;
    }

    while (1)
    {
        int client_fd = accept(g_server.server_socket_fd, NULL, NULL);
        if (client_fd < 0)
        {
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                log_warn("accept failed");
            return;
        }

        if (set_socket_non_blocking(client_fd) != 0)
        {
            close(client_fd);
            continue;
        }

        ServerClientSession *session = NULL;
        int slot = -1;
        for (int i = 0; i < MQTT_SERVER_MAX_CLIENTS; ++i)
        {
            if (!g_server.client_list[i].is_connected)
            {
                session = &g_server.client_list[i];
                slot = i;
                break;
            }
        }

        if (!session)
        {
            close(client_fd);
            log_warn("client rejected: max clients reached");
            return;
        }

        session->socket_fd = client_fd;
        session->is_connected = true;
        session->protocol_level = 4U;
        snprintf((char *)session->client_id, sizeof(session->client_id), "client_%d", slot);

        if (event_loop_add_fd(client_fd, on_client_event, (void *)session, EPOLLIN | EPOLLET | EPOLLRDHUP) != 0)
        {
            log_warn("client epoll add failed: %s", session->client_id);
            server_reset_client(session);
            return;
        }

        log_info("client connected: %s (fd=%d)", session->client_id, client_fd);
    }
}

/**************************************************************************************************
 * @brief       Publish a message to a client
 *************************************************************************************************/
static bool server_send_to_client(ServerClientSession *session, const uint8_t *publish_topic,
                                  const uint8_t *payload, uint32_t len, uint8_t qos)
{
    if (!session || !session->is_connected || !payload || len == 0)
    {
        return false;
    }
    return mqtt_send_publish_json(session, publish_topic, payload, len,
                                  session->protocol_level, qos);
}

/**************************************************************************************************
 * @brief       Drain the TX queue and fan messages out to matching clients
 *************************************************************************************************/
static int server_send_from_tx_queue(void)
{
    int fanout_count = 0;

    while (msg_queue_count(&g_server.tx_queue) > 0)
    {
        uint8_t target[MQTT_SERVER_CLIENT_ID_LEN] = {0};
        uint8_t queued_topic[MQTT_SERVER_TOPIC_LEN] = {0};
        uint8_t *payload = NULL;
        uint32_t len = 0;
        uint8_t publish_topic[MQTT_SERVER_TOPIC_LEN] = {0};
        bool has_publish_topic = false;
        bool is_broadcast;
        bool is_topic_fanout;
        bool has_sender_id;
        int i;
        if (msg_dequeue(&g_server.tx_queue, target, &payload, &len, queued_topic, sizeof(queued_topic)) != 0)
        {
            continue;
        }
        is_broadcast = (target[0] == '\0');
        is_topic_fanout = (queued_topic[0] != '\0');
        has_sender_id = !is_broadcast;
        if (queued_topic[0] != '\0')
        {
            mqtt_copy_text(publish_topic, sizeof(publish_topic), queued_topic);
            has_publish_topic = true;
        }
        else
        {
            log_warn("mqtt tx drop: queue item missing wire topic");
            free(payload);
            continue;
        }
        for (i = 0; i < MQTT_SERVER_MAX_CLIENTS; ++i)
        {
            ServerClientSession *session = &g_server.client_list[i];
            uint8_t delivery_qos = 0U;
            if (!session->is_connected)
            {
                continue;
            }
            if ((is_broadcast || is_topic_fanout) && has_publish_topic &&
                !mqtt_session_matches_topic(session, publish_topic))
            {
                continue;
            }
            /* No-local echo: sender's own publish is never fanned back to itself. */
            if (has_sender_id && strcmp((const char *)session->client_id, (const char *)target) == 0)
            {
                continue;
            }
            /* Unicast: deliver only to the connection matching target */
            if (!is_broadcast && !is_topic_fanout &&
                strcmp((const char *)session->client_id, (const char *)target) != 0)
            {
                continue;
            }

            /* Deliver at the QoS granted by the receiving subscription. */
            delivery_qos = mqtt_session_topic_qos(session, publish_topic);
            if (server_send_to_client(session, publish_topic, payload, len, delivery_qos))
            {
                fanout_count++;
            }
        }
        free(payload);
    }

    return fanout_count;
}

/**************************************************************************************************
 * @brief       epoll callback: TX queue has pending data
 *************************************************************************************************/
static void on_tx_queue_event(int fd, uint32_t events, void *userdata)
{
    uint64_t val = 0;

    (void)userdata;

    if ((events & EPOLLIN) != 0U)
    {
        ssize_t r = read(fd, &val, sizeof(val));
        (void)r;
    }

    (void)server_send_from_tx_queue();
}

/**************************************************************************************************
 * @brief       Retransmit in-flight QoS1 packets whose retry timer has expired
 *************************************************************************************************/
static void server_retry_qos1_inflight(void)
{
    uint64_t now = server_monotonic_ms();

    for (int i = 0; i < MQTT_SERVER_MAX_CLIENTS; ++i)
    {
        ServerClientSession *session = &g_server.client_list[i];
        PendingQos1Node **link = &session->qos1_inflight;

        while (session->is_connected && *link != NULL)
        {
            PendingQos1Node *node = *link;
            if (now - node->last_send_ms < MQTT_QOS1_RETRY_MS)
            {
                link = &node->next;
                continue;
            }
            if (node->retry_count >= MQTT_QOS1_MAX_RETRIES)
            {
                log_warn("mqtt QoS1 delivery failed: client=%s packet_id=%u retries=%u",
                         session->client_id, node->packet_id, node->retry_count);
                *link = node->next;
                free(node->packet);
                free(node);
                continue;
            }

            node->packet[0] |= 0x08U; /* DUP */
            node->retry_count++;
            node->last_send_ms = now;
            if (socket_send_nonblock(session, node->packet, node->packet_len))
            {
                log_warn("mqtt QoS1 retry: client=%s packet_id=%u retry=%u",
                         session->client_id, node->packet_id, node->retry_count);
            }
            else
            {
                log_warn("mqtt QoS1 retry send failed: client=%s packet_id=%u retry=%u",
                         session->client_id, node->packet_id, node->retry_count);
            }
            link = &node->next;
        }
    }
}

/**************************************************************************************************
 * @brief       epoll callback: periodic QoS1 retransmission timer tick
 *************************************************************************************************/
static void on_qos1_retry_timer_event(int fd, uint32_t events, void *userdata)
{
    uint64_t expirations = 0U;

    (void)userdata;
    if ((events & EPOLLIN) != 0U)
    {
        ssize_t r = read(fd, &expirations, sizeof(expirations));
        (void)r;
        server_retry_qos1_inflight();
    }
}

/**************************************************************************************************
 * @brief       Fallback handler on epoll timeout: flush TX queue and retry QoS1 packets
 *************************************************************************************************/
static void server_on_event_loop_timeout(void)
{
    (void)server_send_from_tx_queue();
    server_retry_qos1_inflight();
}

/**************************************************************************************************
 * @brief       Initialize the broker: TCP listener, epoll loop, eventfd and QoS1 retry timer
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int mqtt_broker_init(void)
{
    memset(&g_server, 0, sizeof(g_server));
    g_server.server_socket_fd = -1;
    g_tx_queue_event_fd = -1;
    g_qos1_retry_timer_fd = -1;

    msg_queue_init(&g_server.tx_queue);

    for (int i = 0; i < MQTT_SERVER_MAX_CLIENTS; ++i)
    {
        g_server.client_list[i].socket_fd = -1;
        g_server.client_list[i].rx_buf = g_server.client_list[i].rx_inline;
        g_server.client_list[i].rx_cap = MQTT_SERVER_RX_INLINE_SIZE;
        g_server.client_list[i].rx_len = 0U;
    }

    if (event_loop_init() != 0)
    {
        log_err("event loop init failed");
        return -1;
    }

    event_loop_set_timeout_cb(server_on_event_loop_timeout);

    if (server_socket_init() != 0)
    {
        event_loop_destroy();
        msg_queue_destroy(&g_server.tx_queue);
        return -1;
    }

    if (event_loop_add_fd(g_server.server_socket_fd, on_accept_event, NULL, EPOLLIN) != 0)
    {
        log_err("epoll add listen fd failed");
        close(g_server.server_socket_fd);
        event_loop_destroy();
        msg_queue_destroy(&g_server.tx_queue);
        return -1;
    }

    g_tx_queue_event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_tx_queue_event_fd < 0)
    {
        log_err("tx queue eventfd create failed");
        event_loop_del_fd(g_server.server_socket_fd);
        close(g_server.server_socket_fd);
        event_loop_destroy();
        msg_queue_destroy(&g_server.tx_queue);
        return -1;
    }

    if (event_loop_add_fd(g_tx_queue_event_fd, on_tx_queue_event, NULL, EPOLLIN) != 0)
    {
        log_err("epoll add tx queue eventfd failed");
        close(g_tx_queue_event_fd);
        event_loop_del_fd(g_server.server_socket_fd);
        close(g_server.server_socket_fd);
        event_loop_destroy();
        msg_queue_destroy(&g_server.tx_queue);
        return -1;
    }

    g_qos1_retry_timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (g_qos1_retry_timer_fd < 0)
    {
        log_err("QoS1 retry timerfd create failed");
        event_loop_del_fd(g_tx_queue_event_fd);
        close(g_tx_queue_event_fd);
        event_loop_del_fd(g_server.server_socket_fd);
        close(g_server.server_socket_fd);
        event_loop_destroy();
        msg_queue_destroy(&g_server.tx_queue);
        return -1;
    }
    {
        struct itimerspec timer_spec;
        memset(&timer_spec, 0, sizeof(timer_spec));
        timer_spec.it_value.tv_sec = 1;
        timer_spec.it_interval.tv_sec = 1;
        if (timerfd_settime(g_qos1_retry_timer_fd, 0, &timer_spec, NULL) != 0 ||
            event_loop_add_fd(g_qos1_retry_timer_fd, on_qos1_retry_timer_event, NULL, EPOLLIN) != 0)
        {
            log_err("QoS1 retry timer setup failed");
            close(g_qos1_retry_timer_fd);
            g_qos1_retry_timer_fd = -1;
            event_loop_del_fd(g_tx_queue_event_fd);
            close(g_tx_queue_event_fd);
            event_loop_del_fd(g_server.server_socket_fd);
            close(g_server.server_socket_fd);
            event_loop_destroy();
            msg_queue_destroy(&g_server.tx_queue);
            return -1;
        }
    }

    g_server.is_module_running = true;
    log_info("mqtt broker init success (epoll event-driven)");
    return 0;
}

/**************************************************************************************************
 * @brief       Run the broker event loop (blocking forever)
 * @return      Never returns on success, -1 on fatal error
 *************************************************************************************************/
int mqtt_broker_run(void)
{
    return event_loop_run();
}

/**************************************************************************************************
 * @brief       Check whether the broker is running
 * @return      true if running
 *************************************************************************************************/
bool mqtt_broker_is_running(void)
{
    return g_server.is_module_running;
}
