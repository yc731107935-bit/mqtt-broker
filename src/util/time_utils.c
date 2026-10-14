/**************************************************************************************************
 * @file        time_utils.c
 * @brief       Timestamp helper.
 *************************************************************************************************/
#include "time_utils.h"

#include <stdio.h>
#include <sys/time.h>
#include <time.h>

/**************************************************************************************************
 * @brief       Write the local time as "YYYY-MM-DD HH:MM:SS.mmm"
 * @param[in]   str Output buffer
 * @param[in]   len Size of the output buffer (at least 12 bytes)
 *************************************************************************************************/
void get_log_timestamp(uint8_t *str, size_t len)
{
    struct timeval tv;
    const struct tm *tmv;

    if (str == NULL || len < 12U)
    {
        return;
    }
    (void)gettimeofday(&tv, NULL);
    tmv = localtime(&tv.tv_sec);
    if (tmv != NULL)
    {
        (void)snprintf((char *)str, len, "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
                       tmv->tm_year + 1900, tmv->tm_mon + 1,
                       tmv->tm_mday, tmv->tm_hour, tmv->tm_min, tmv->tm_sec,
                       (long)(tv.tv_usec / 1000));
    }
    else
    {
        str[0] = '\0';
    }
}
