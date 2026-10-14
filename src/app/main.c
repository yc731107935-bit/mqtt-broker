/**************************************************************************************************
 * @file        main.c
 * @brief       Standalone MQTT 3.1.1 broker entry point: initialize the broker and run the
 *              event loop.
 *************************************************************************************************/
#include <stdio.h>
#include "log.h"
#include "mqtt_broker.h"

int main(void)
{
    printf("mqtt-broker v1.0\n");
    log_set_level(LOG_INFO);
    if (mqtt_broker_init() != 0)
    {
        return 1;
    }

    mqtt_broker_run();

    return 0;
}
