/**************************************************************************************************
 * @file        message_queue.c
 * @brief       Ring-buffer message queue implementation.
 *************************************************************************************************/

#include "message_queue.h"
#include <stdlib.h>
#include <string.h>

/**************************************************************************************************
 * @brief       Copy a NUL-terminated string with truncation into a fixed-size buffer
 * @param[in]   dst      Destination buffer
 * @param[in]   dst_size Size of the destination buffer
 * @param[in]   src      Source string (NULL yields an empty string)
 *************************************************************************************************/
static void queue_copy_text(uint8_t *dst, size_t dst_size, const uint8_t *src)
{
    size_t copy_len = 0U;

    if (dst == NULL || dst_size == 0U)
    {
        return;
    }

    if (src == NULL)
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
 * @brief       Initialize a message queue to the empty state
 * @param[in]   queue Queue to initialize
 *************************************************************************************************/
void msg_queue_init(MsgQueue *queue)
{
    if (queue == NULL)
    {
        return;
    }

    memset(queue, 0, sizeof(MsgQueue));
}

/**************************************************************************************************
 * @brief       Release every queued node and reset the queue
 * @param[in]   queue Queue to destroy
 *************************************************************************************************/
void msg_queue_destroy(MsgQueue *queue)
{
    uint32_t i = 0U;
    MsgNode *node = NULL;

    if (queue == NULL)
    {
        return;
    }

    for (i = 0U; i < queue->count; i++)
    {
        uint32_t idx = (queue->head + i) % MSG_QUEUE_MAX_SIZE;
        node = queue->node_list[idx];

        if (node != NULL)
        {
            if (node->payload != NULL)
            {
                free(node->payload);
                node->payload = NULL;
            }
            free(node);
            queue->node_list[idx] = NULL;
        }
    }

    memset(queue, 0, sizeof(MsgQueue));
}

/**************************************************************************************************
 * @brief       Append one message node to the queue tail
 * @param[in]   queue            Target queue
 * @param[in]   target_id        Sender client id (may be NULL for broadcast)
 * @param[in]   payload          Payload bytes (copied into the node)
 * @param[in]   len              Payload length in bytes
 * @param[in]   mqtt_wire_topic  Original MQTT wire topic (optional)
 * @return      0 on success, -1 on invalid arguments or allocation failure, -2 when full
 *************************************************************************************************/
int msg_enqueue(MsgQueue *queue, const uint8_t *target_id, const uint8_t *payload, uint32_t len, const uint8_t *mqtt_wire_topic)
{
    MsgNode *node = NULL;

    if (queue == NULL || payload == NULL || len == 0U)
    {
        return -1;
    }

    if (queue->count >= MSG_QUEUE_MAX_SIZE)
    {
        return -2;
    }

    node = (MsgNode *)malloc(sizeof(MsgNode));
    if (node == NULL)
    {
        return -1;
    }

    memset(node, 0, sizeof(MsgNode));

    if (mqtt_wire_topic != NULL && mqtt_wire_topic[0] != '\0')
    {
        queue_copy_text(node->mqtt_wire_topic, sizeof(node->mqtt_wire_topic), mqtt_wire_topic);
    }

    node->payload = (uint8_t *)malloc(len + 1U);
    if (node->payload == NULL)
    {
        free(node);
        return -1;
    }

    if (target_id != NULL)
    {
        queue_copy_text(node->target_id, sizeof(node->target_id), target_id);
    }
    else
    {
        memset(node->target_id, 0, sizeof(node->target_id));
    }

    memcpy(node->payload, payload, len);
    node->payload[len] = '\0';
    node->len = len;

    queue->node_list[queue->tail] = node;
    queue->tail = (queue->tail + 1U) % MSG_QUEUE_MAX_SIZE;
    queue->count++;

    return 0;
}

/**************************************************************************************************
 * @brief       Pop one message node from the queue head; payload ownership moves to the caller
 * @param[in]   queue                   Source queue
 * @param[in]   target_id               Output buffer for the sender client id (optional)
 * @param[in]   payload                 Output pointer for the payload (caller must free)
 * @param[in]   len                     Output payload length
 * @param[in]   mqtt_wire_topic_out     Output buffer for the wire topic (optional)
 * @param[in]   mqtt_wire_topic_out_sz  Size of the wire topic buffer
 * @return      0 on success, -1 on invalid arguments or internal error, -2 when empty
 *************************************************************************************************/
int msg_dequeue(MsgQueue *queue, uint8_t *target_id, uint8_t **payload, uint32_t *len, uint8_t *mqtt_wire_topic_out, size_t mqtt_wire_topic_out_sz)
{
    MsgNode *node = NULL;

    if (queue == NULL || payload == NULL || len == NULL)
    {
        return -1;
    }

    if (queue->count == 0U)
    {
        return -2;
    }

    node = queue->node_list[queue->head];
    if (node == NULL)
    {
        queue->head = (queue->head + 1U) % MSG_QUEUE_MAX_SIZE;
        queue->count--;
        return -1;
    }

    if (target_id != NULL)
    {
        queue_copy_text(target_id, 32U, node->target_id);
    }

    if (mqtt_wire_topic_out != NULL && mqtt_wire_topic_out_sz > 0U)
    {
        queue_copy_text(mqtt_wire_topic_out, mqtt_wire_topic_out_sz, node->mqtt_wire_topic);
    }

    *payload = node->payload;
    *len = node->len;

    free(node);
    queue->node_list[queue->head] = NULL;

    queue->head = (queue->head + 1U) % MSG_QUEUE_MAX_SIZE;
    queue->count--;

    return 0;
}

/**************************************************************************************************
 * @brief       Get the number of queued messages
 * @param[in]   queue Target queue
 * @return      Current message count
 *************************************************************************************************/
uint32_t msg_queue_count(const MsgQueue *queue)
{
    if (queue == NULL)
    {
        return 0U;
    }

    return queue->count;
}
