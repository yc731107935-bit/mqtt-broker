#ifndef MQTT_BROKER_H
#define MQTT_BROKER_H

/**************************************************************************************************
 * @file        mqtt_broker.h
 * @brief       Standalone MQTT broker: TCP MQTT protocol handling and subscription
 *              forwarding only; no business logic, no message bus.
 *************************************************************************************************/
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "message_queue.h"

#define MQTT_SERVER_MAX_CLIENTS        8
/** Default inline RX buffer; moves to heap when exceeded, capped by MQTT_SERVER_RX_MAX_SIZE. */
#define MQTT_SERVER_RX_INLINE_SIZE     2048
#define MQTT_SERVER_RX_MAX_SIZE        (64 * 1024)
#define MQTT_SERVER_CLIENT_ID_LEN      32
#define MQTT_SERVER_MAX_SUBSCRIPTIONS  16
#define MQTT_SERVER_TOPIC_LEN          128

/** Pending TX buffer node (linked list). */
typedef struct PendingTxNode
{
    uint8_t *data;
    size_t total_len;
    size_t sent_offset;
    struct PendingTxNode *next;
} PendingTxNode;

/** QoS1 packet already sent but not yet acknowledged by PUBACK. */
typedef struct PendingQos1Node
{
    uint16_t packet_id;
    uint8_t *packet;
    size_t packet_len;
    uint64_t last_send_ms;
    uint8_t retry_count;
    struct PendingQos1Node *next;
} PendingQos1Node;

typedef struct
{
    int socket_fd;
    bool is_connected;
    uint8_t protocol_level;
    uint8_t client_id[MQTT_SERVER_CLIENT_ID_LEN];
    uint8_t subscriptions[MQTT_SERVER_MAX_SUBSCRIPTIONS][MQTT_SERVER_TOPIC_LEN];
    uint8_t subscription_qos[MQTT_SERVER_MAX_SUBSCRIPTIONS];
    uint8_t subscription_count;
    /** Pending TX list (buffered while the socket write buffer is full). */
    PendingTxNode *pending_tx_head;
    PendingTxNode *pending_tx_tail;
    PendingQos1Node *qos1_inflight;
    uint16_t next_out_packet_id;
    /** EPOLLET partial-frame reassembly buffer: rx_inline by default, heap when exceeded. */
    uint8_t rx_inline[MQTT_SERVER_RX_INLINE_SIZE];
    uint8_t *rx_buf;
    size_t rx_cap;
    size_t rx_len;
} ServerClientSession;

typedef struct
{
    int server_socket_fd;
    ServerClientSession client_list[MQTT_SERVER_MAX_CLIENTS];
    MsgQueue tx_queue;
    bool is_module_running;
} MqttServerContext;

/**************************************************************************************************
 * @brief       Initialize the broker (TCP listener, epoll loop, eventfd, QoS1 retry timer)
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int mqtt_broker_init(void);

/**************************************************************************************************
 * @brief       Run the broker event loop (blocking forever)
 * @return      Never returns on success, -1 on fatal error
 *************************************************************************************************/
int mqtt_broker_run(void);

/**************************************************************************************************
 * @brief       Check whether the broker is running
 * @return      true if running
 *************************************************************************************************/
bool mqtt_broker_is_running(void);

#endif
