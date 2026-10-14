/**************************************************************************************************
 * @file        log.h
 * @brief       Lightweight thread-safe logger: leveled output to stderr with timestamps.
 *************************************************************************************************/
#ifndef BROKER_LOG_H
#define BROKER_LOG_H

#include <stdarg.h>
#include <stdint.h>

#define LOG_BUF_SIZE       1024
#define LOG_TIME_SIZE      32
#define LOG_LEVEL_DEFAULT  LOG_INFO

typedef enum
{
    LOG_EMERG   = 0,
    LOG_ALERT   = 1,
    LOG_CRIT    = 2,
    LOG_ERR     = 3,
    LOG_WARNING = 4,
    LOG_NOTICE  = 5,
    LOG_INFO    = 6,
    LOG_DEBUG   = 7
} LogLevel;

/**************************************************************************************************
 * @brief       Set the global log level
 * @param[in]   level Minimum level to output
 *************************************************************************************************/
void log_set_level(LogLevel level);

/**************************************************************************************************
 * @brief       Format one log line with timestamp and level, then write it to stderr
 * @param[in]   level  Log level
 * @param[in]   format printf-style format string
 * @return      0 on success, -1 on formatting failure
 *************************************************************************************************/
int log_print(LogLevel level, const char *format, ...);

#define log_emerg(fmt, ...)   log_print(LOG_EMERG,   fmt, ##__VA_ARGS__)
#define log_alert(fmt, ...)   log_print(LOG_ALERT,   fmt, ##__VA_ARGS__)
#define log_crit(fmt, ...)    log_print(LOG_CRIT,    fmt, ##__VA_ARGS__)
#define log_err(fmt, ...)     log_print(LOG_ERR,     fmt, ##__VA_ARGS__)
#define log_warn(fmt, ...)    log_print(LOG_WARNING, fmt, ##__VA_ARGS__)
#define log_notice(fmt, ...)  log_print(LOG_NOTICE,  fmt, ##__VA_ARGS__)
#define log_info(fmt, ...)    log_print(LOG_INFO,    fmt, ##__VA_ARGS__)
#define log_debug(fmt, ...)   log_print(LOG_DEBUG,   fmt, ##__VA_ARGS__)

#endif
