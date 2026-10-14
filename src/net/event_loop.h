/**************************************************************************************************
 * @file        event_loop.h
 * @brief       Generic epoll-based event loop with a callback-driven API.
 *************************************************************************************************/
#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <stdbool.h>
#include <stdint.h>
#include <sys/epoll.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Maximum number of events handled per loop iteration. */
#define EVENT_LOOP_MAX_EVENTS 16

/** epoll_wait timeout in ms; on timeout all registered fds are re-polled as a fallback. */
#define EVENT_LOOP_TIMEOUT_MS 60000

/**************************************************************************************************
 * @brief       Event callback prototype
 * @param[in]   fd        File descriptor the event occurred on
 * @param[in]   events    epoll event mask (EPOLLIN, EPOLLOUT, EPOLLERR, ...)
 * @param[in]   userdata  User pointer supplied at registration time
 *************************************************************************************************/
typedef void (*event_loop_cb)(int fd, uint32_t events, void *userdata);

/** Extra fallback callback fired on epoll timeout (may be NULL). */
typedef void (*event_loop_timeout_cb)(void);

/**************************************************************************************************
 * @brief       Create the epoll instance; must run before any other event_loop call
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int event_loop_init(void);

/**************************************************************************************************
 * @brief       Destroy the loop: free registered items and close the epoll fd
 *************************************************************************************************/
void event_loop_destroy(void);

/**************************************************************************************************
 * @brief       Register a file descriptor and its callback with the event loop
 * @param[in]   fd        File descriptor to monitor
 * @param[in]   callback  Callback fired on events (must not be NULL)
 * @param[in]   userdata  User pointer passed back to the callback
 * @param[in]   events    Interested epoll event mask (EPOLLIN | EPOLLOUT | EPOLLET, ...)
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int event_loop_add_fd(int fd, event_loop_cb callback, void *userdata, uint32_t events);

/**************************************************************************************************
 * @brief       Change the epoll event mask of a registered file descriptor
 * @param[in]   fd     File descriptor
 * @param[in]   events New event mask
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int event_loop_mod_fd(int fd, uint32_t events);

/**************************************************************************************************
 * @brief       Remove a file descriptor from the event loop
 * @param[in]   fd File descriptor to remove
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int event_loop_del_fd(int fd);

/**************************************************************************************************
 * @brief       Register the extra callback fired on epoll timeout
 * @param[in]   callback Timeout callback (NULL to clear)
 *************************************************************************************************/
void event_loop_set_timeout_cb(event_loop_timeout_cb callback);

/**************************************************************************************************
 * @brief       Run the main event loop (blocking)
 *
 * epoll_wait blocks for EVENT_LOOP_TIMEOUT_MS; on timeout every registered fd is polled
 * again as a fallback against lost edge-triggered events.
 *
 * @return      Never returns on success, -1 on fatal error
 *************************************************************************************************/
int event_loop_run(void);

#ifdef __cplusplus
}
#endif

#endif /* EVENT_LOOP_H */
