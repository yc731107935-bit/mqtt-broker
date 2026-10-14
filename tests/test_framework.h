/**************************************************************************************************
 * @file        test_framework.h
 * @brief       Minimal assertion-based unit test framework (single translation unit only).
 *************************************************************************************************/
#ifndef TEST_FRAMEWORK_H
#define TEST_FRAMEWORK_H

#include <stdio.h>

static int g_tests_run = 0;
static int g_tests_failed = 0;

/**************************************************************************************************
 * @brief       Assert a condition; failures are counted and reported with file/line
 * @param[in]   cond Condition to check
 *************************************************************************************************/
#define TEST_ASSERT(cond)                                                     \
    do                                                                        \
    {                                                                         \
        g_tests_run++;                                                        \
        if (!(cond))                                                          \
        {                                                                     \
            g_tests_failed++;                                                 \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
        }                                                                     \
    } while (0)

/**************************************************************************************************
 * @brief       Assert two integers are equal
 * @param[in]   a First value
 * @param[in]   b Expected value
 *************************************************************************************************/
#define TEST_ASSERT_EQ(a, b) TEST_ASSERT((a) == (b))

/**************************************************************************************************
 * @brief       Print the test summary; usable as the process exit code
 * @return      0 when all tests passed, 1 otherwise
 *************************************************************************************************/
#define TEST_SUMMARY()                                                            \
    (printf("%d/%d checks passed\n", g_tests_run - g_tests_failed, g_tests_run),  \
     g_tests_failed ? 1 : 0)

#endif
