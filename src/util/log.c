/**************************************************************************************************
 * @file        log.c
 * @brief       Lightweight logger: leveled output to stderr with timestamps.
 *************************************************************************************************/
#include "log.h"
#include "time_utils.h"

#include <pthread.h>
#include <stdio.h>

static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;
static LogLevel g_log_level = LOG_LEVEL_DEFAULT;

static const char *g_log_level_str[] = {
    [LOG_EMERG]   = "EMERG",
    [LOG_ALERT]   = "ALERT",
    [LOG_CRIT]    = "CRIT",
    [LOG_ERR]     = "ERR",
    [LOG_WARNING] = "WARN",
    [LOG_NOTICE]  = "NOTICE",
    [LOG_INFO]    = "INFO",
    [LOG_DEBUG]   = "DEBUG",
};

/**************************************************************************************************
 * @brief       Set the global log level
 * @param[in]   level Minimum level to output
 *************************************************************************************************/
void log_set_level(LogLevel level)
{
    if (level >= LOG_EMERG && level <= LOG_DEBUG)
    {
        pthread_mutex_lock(&g_log_mutex);
        g_log_level = level;
        pthread_mutex_unlock(&g_log_mutex);
    }
}

/**************************************************************************************************
 * @brief       Format one log line with timestamp and level, then write it to stderr
 * @param[in]   level  Log level
 * @param[in]   format printf-style format string
 * @return      0 on success, -1 on formatting failure
 *************************************************************************************************/
int log_print(LogLevel level, const char *format, ...)
{
    char msg_buf[LOG_BUF_SIZE] = {0};
    char time_str[LOG_TIME_SIZE] = {0};
    va_list arg_ptr;
    int print_len = 0;

    if (level > g_log_level)
    {
        return 0;
    }

    va_start(arg_ptr, format);
    print_len = vsnprintf(msg_buf, sizeof(msg_buf), format, arg_ptr);
    va_end(arg_ptr);

    if (print_len < 0)
    {
        return -1;
    }

    get_log_timestamp((uint8_t *)time_str, sizeof(time_str));
    const char *level_name = g_log_level_str[level];

    pthread_mutex_lock(&g_log_mutex);
    fprintf(stderr, "\033[36m[mqtt-broker]\033[0m %s [%s] %s\n", time_str, level_name, msg_buf);
    fflush(stderr);
    pthread_mutex_unlock(&g_log_mutex);

    return 0;
}
