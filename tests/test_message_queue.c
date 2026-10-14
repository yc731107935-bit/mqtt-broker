/**************************************************************************************************
 * @file        test_message_queue.c
 * @brief       Unit tests for the ring-buffer message queue (src/broker/message_queue.c).
 *************************************************************************************************/
#include "message_queue.h"
#include "test_framework.h"

#include <stdlib.h>
#include <string.h>

/**************************************************************************************************
 * @brief       Init/destroy cycle keeps the queue empty and consistent
 *************************************************************************************************/
static void test_init_destroy(void)
{
    MsgQueue q;

    msg_queue_init(&q);
    TEST_ASSERT_EQ((int)msg_queue_count(&q), 0);
    msg_queue_destroy(&q);
    TEST_ASSERT_EQ((int)msg_queue_count(&q), 0);

    msg_queue_init(NULL);
    msg_queue_destroy(NULL);
    msg_queue_count(NULL);
    TEST_ASSERT(1);
}

/**************************************************************************************************
 * @brief       Enqueue followed by dequeue returns the same payload, id and topic
 *************************************************************************************************/
static void test_enqueue_dequeue(void)
{
    MsgQueue q;
    uint8_t target[32] = {0};
    uint8_t topic[64] = {0};
    uint8_t *payload = NULL;
    uint32_t len = 0;
    const uint8_t msg[] = "hello-world";

    msg_queue_init(&q);
    TEST_ASSERT_EQ(msg_enqueue(&q, (const uint8_t *)"clientA", msg, sizeof(msg) - 1U,
                               (const uint8_t *)"sensors/temp"),
                   0);
    TEST_ASSERT_EQ((int)msg_queue_count(&q), 1);

    TEST_ASSERT_EQ(msg_dequeue(&q, target, &payload, &len, topic, sizeof(topic)), 0);
    TEST_ASSERT(strcmp((const char *)target, "clientA") == 0);
    TEST_ASSERT(strcmp((const char *)topic, "sensors/temp") == 0);
    TEST_ASSERT_EQ((int)len, (int)(sizeof(msg) - 1U));
    TEST_ASSERT(payload != NULL && memcmp(payload, msg, len) == 0);
    free(payload);

    TEST_ASSERT_EQ((int)msg_queue_count(&q), 0);
    TEST_ASSERT_EQ(msg_dequeue(&q, target, &payload, &len, topic, sizeof(topic)), -2);
    msg_queue_destroy(&q);
}

/**************************************************************************************************
 * @brief       FIFO order is preserved across mixed enqueue/dequeue operations
 *************************************************************************************************/
static void test_fifo_order(void)
{
    MsgQueue q;
    uint8_t *payload = NULL;
    uint32_t len = 0;

    msg_queue_init(&q);
    for (uint8_t i = 0; i < 4U; i++)
    {
        TEST_ASSERT_EQ(msg_enqueue(&q, NULL, &i, 1U, (const uint8_t *)"t"), 0);
    }
    for (uint8_t i = 0; i < 4U; i++)
    {
        TEST_ASSERT_EQ(msg_dequeue(&q, NULL, &payload, &len, NULL, 0U), 0);
        TEST_ASSERT_EQ((int)len, 1);
        TEST_ASSERT_EQ((int)payload[0], (int)i);
        free(payload);
    }
    msg_queue_destroy(&q);
}

/**************************************************************************************************
 * @brief       The queue rejects the 33rd element and invalid arguments
 *************************************************************************************************/
static void test_full_and_invalid(void)
{
    MsgQueue q;
    uint8_t *payload = NULL;
    uint32_t len = 0;
    const uint8_t msg[] = "x";

    msg_queue_init(&q);
    for (int i = 0; i < MSG_QUEUE_MAX_SIZE; i++)
    {
        TEST_ASSERT_EQ(msg_enqueue(&q, NULL, msg, 1U, (const uint8_t *)"t"), 0);
    }
    TEST_ASSERT_EQ(msg_enqueue(&q, NULL, msg, 1U, (const uint8_t *)"t"), -2);
    TEST_ASSERT_EQ((int)msg_queue_count(&q), MSG_QUEUE_MAX_SIZE);

    /* Invalid arguments must not crash or enqueue */
    TEST_ASSERT_EQ(msg_enqueue(NULL, NULL, msg, 1U, NULL), -1);
    TEST_ASSERT_EQ(msg_enqueue(&q, NULL, NULL, 1U, NULL), -1);
    TEST_ASSERT_EQ(msg_enqueue(&q, NULL, msg, 0U, NULL), -1);
    TEST_ASSERT_EQ(msg_dequeue(&q, NULL, NULL, &len, NULL, 0U), -1);
    TEST_ASSERT_EQ(msg_dequeue(NULL, NULL, &payload, &len, NULL, 0U), -1);
    TEST_ASSERT_EQ((int)msg_queue_count(&q), MSG_QUEUE_MAX_SIZE);

    /* Long topics are truncated, not overflown */
    uint8_t long_topic[MSG_QUEUE_WIRE_TOPIC_LEN + 32];
    memset(long_topic, 'a', sizeof(long_topic));
    long_topic[sizeof(long_topic) - 1U] = '\0';
    msg_queue_destroy(&q);
    msg_queue_init(&q);
    TEST_ASSERT_EQ(msg_enqueue(&q, NULL, msg, 1U, long_topic), 0);
    uint8_t topic[MSG_QUEUE_WIRE_TOPIC_LEN + 32] = {0};
    TEST_ASSERT_EQ(msg_dequeue(&q, NULL, &payload, &len, topic, sizeof(topic)), 0);
    TEST_ASSERT(strlen((const char *)topic) == MSG_QUEUE_WIRE_TOPIC_LEN - 1U);
    free(payload);
    msg_queue_destroy(&q);
}

int main(void)
{
    test_init_destroy();
    test_enqueue_dequeue();
    test_fifo_order();
    test_full_and_invalid();
    return TEST_SUMMARY();
}
