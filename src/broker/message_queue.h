/**************************************************************************************************
 * @file        message_queue.h
 * @brief       Ring-buffer message queue used for TX fan-out.
 *************************************************************************************************/
#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#define MSG_QUEUE_MAX_SIZE      32
#define MSG_QUEUE_WIRE_TOPIC_LEN 128

/** One queued message. */
typedef struct
{
    uint8_t target_id[32];
    uint8_t mqtt_wire_topic[MSG_QUEUE_WIRE_TOPIC_LEN];
    uint8_t *payload;
    uint32_t len;
} MsgNode;

/** Ring buffer of message nodes. */
typedef struct
{
    MsgNode *node_list[MSG_QUEUE_MAX_SIZE];
    uint32_t head;
    uint32_t tail;
    uint32_t count;
} MsgQueue;

/**************************************************************************************************
 * @brief       Initialize a message queue to the empty state
 * @param[in]   queue Queue to initialize
 *************************************************************************************************/
void msg_queue_init(MsgQueue *queue);

/**************************************************************************************************
 * @brief       Release every queued node and reset the queue
 * @param[in]   queue Queue to destroy
 *************************************************************************************************/
void msg_queue_destroy(MsgQueue *queue);

/**************************************************************************************************
 * @brief       Append one message node to the queue tail
 * @param[in]   queue            Target queue
 * @param[in]   target_id        Sender client id (may be NULL for broadcast)
 * @param[in]   payload          Payload bytes (copied into the node)
 * @param[in]   len              Payload length in bytes
 * @param[in]   mqtt_wire_topic  Original MQTT wire topic (optional)
 * @return      0 on success, -1 on invalid arguments or allocation failure, -2 when full
 *************************************************************************************************/
int msg_enqueue(MsgQueue *queue, const uint8_t *target_id, const uint8_t *payload, uint32_t len, const uint8_t *mqtt_wire_topic);

/**************************************************************************************************
 * @brief       Pop one message node from the queue head; payload ownership moves to the caller
 * @param[in]   queue                   Source queue
 * @param[out]  target_id               Output buffer for the sender client id (optional)
 * @param[out]  payload                 Output pointer for the payload (caller must free)
 * @param[out]  len                     Output payload length
 * @param[out]  mqtt_wire_topic_out     Output buffer for the wire topic (optional)
 * @param[in]   mqtt_wire_topic_out_sz  Size of the wire topic buffer
 * @return      0 on success, -1 on invalid arguments or internal error, -2 when empty
 *************************************************************************************************/
int msg_dequeue(MsgQueue *queue, uint8_t *target_id, uint8_t **payload, uint32_t *len, uint8_t *mqtt_wire_topic_out, size_t mqtt_wire_topic_out_sz);

/**************************************************************************************************
 * @brief       Get the number of queued messages
 * @param[in]   queue Target queue
 * @return      Current message count
 *************************************************************************************************/
uint32_t msg_queue_count(const MsgQueue *queue);

#endif
