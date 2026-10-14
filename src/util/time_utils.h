/**************************************************************************************************
 * @file        time_utils.h
 * @brief       Timestamp helper.
 *************************************************************************************************/
#ifndef BROKER_TIME_H
#define BROKER_TIME_H

#include <stddef.h>
#include <stdint.h>

/**************************************************************************************************
 * @brief       Write the local time as "YYYY-MM-DD HH:MM:SS.mmm" (used by the logger)
 * @param[in]   str Output buffer
 * @param[in]   len Size of the output buffer
 *************************************************************************************************/
void get_log_timestamp(uint8_t *str, size_t len);

#endif
