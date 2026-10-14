/**************************************************************************************************
 * @file        event_loop.c
 * @brief       Generic epoll-based event loop implementation.
 *************************************************************************************************/
#include "event_loop.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/** Upper bound of registered fds (8 clients + listener + eventfd/timerfd). */
#define EVENT_LOOP_MAX_REGISTERED_FDS 32

/** Global event loop context. */
typedef struct
{
    int epoll_fd;
    bool initialized;
    event_loop_timeout_cb timeout_cb;
    struct epoll_event events[EVENT_LOOP_MAX_EVENTS];
} EventLoopContext;

static EventLoopContext g_evloop = {
    .epoll_fd = -1,
    .initialized = false,
    .timeout_cb = NULL,
};

typedef struct
{
    event_loop_cb callback;
    void *userdata;
    int fd;
} EventLoopItem;

typedef struct EventLoopItemEntry
{
    EventLoopItem *item;
    struct EventLoopItemEntry *next;
} EventLoopItemEntry;

static EventLoopItemEntry *g_item_list = NULL;

/**************************************************************************************************
 * @brief       Find the registered item for a file descriptor
 * @param[in]   fd File descriptor to look up
 *************************************************************************************************/
static EventLoopItem *event_loop_find_item(int fd)
{
    EventLoopItemEntry *entry = g_item_list;

    while (entry != NULL)
    {
        if (entry->item != NULL && entry->item->fd == fd)
        {
            return entry->item;
        }
        entry = entry->next;
    }

    return NULL;
}

/**************************************************************************************************
 * @brief       Link an item into the registered-item list
 * @param[in]   item Item to register
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
static int event_loop_register_item(EventLoopItem *item)
{
    EventLoopItemEntry *entry = NULL;

    if (item == NULL)
    {
        return -1;
    }

    entry = (EventLoopItemEntry *)malloc(sizeof(EventLoopItemEntry));
    if (entry == NULL)
    {
        return -1;
    }

    entry->item = item;
    entry->next = g_item_list;
    g_item_list = entry;
    return 0;
}

/**************************************************************************************************
 * @brief       Unlink and free the registered item for a file descriptor
 * @param[in]   fd File descriptor to remove
 *************************************************************************************************/
static void event_loop_unregister_item(int fd)
{
    EventLoopItemEntry **entry_ptr = &g_item_list;

    while (*entry_ptr != NULL)
    {
        EventLoopItemEntry *entry = *entry_ptr;

        if (entry->item != NULL && entry->item->fd == fd)
        {
            free(entry->item);
            *entry_ptr = entry->next;
            free(entry);
            return;
        }

        entry_ptr = &entry->next;
    }
}

/**************************************************************************************************
 * @brief       Free every registered item (used on loop destruction)
 *************************************************************************************************/
static void event_loop_clear_items(void)
{
    while (g_item_list != NULL)
    {
        EventLoopItemEntry *entry = g_item_list;

        g_item_list = entry->next;
        free(entry->item);
        free(entry);
    }
}

/**************************************************************************************************
 * @brief       Fallback poll: invoke every registered callback once (epoll timeout watchdog)
 *************************************************************************************************/
static void event_loop_poll_registered(void)
{
    EventLoopItem *snapshot[EVENT_LOOP_MAX_REGISTERED_FDS];
    EventLoopItemEntry *entry = g_item_list;
    int count = 0;
    int i = 0;

    while (entry != NULL && count < EVENT_LOOP_MAX_REGISTERED_FDS)
    {
        if (entry->item != NULL && entry->item->callback != NULL)
        {
            snapshot[count++] = entry->item;
        }
        entry = entry->next;
    }

    for (i = 0; i < count; i++)
    {
        snapshot[i]->callback(snapshot[i]->fd, EPOLLIN | EPOLLOUT, snapshot[i]->userdata);
    }
}

/**************************************************************************************************
 * @brief       Register the extra callback fired on epoll timeout (NULL to clear)
 * @param[in]   callback Timeout callback, may be NULL
 *************************************************************************************************/
void event_loop_set_timeout_cb(event_loop_timeout_cb callback)
{
    g_evloop.timeout_cb = callback;
}

/**************************************************************************************************
 * @brief       Create the epoll instance; must run before any other event_loop call
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int event_loop_init(void)
{
    if (g_evloop.initialized)
    {
        return 0;
    }

    g_evloop.epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (g_evloop.epoll_fd < 0)
    {
        return -1;
    }

    g_evloop.initialized = true;
    return 0;
}

/**************************************************************************************************
 * @brief       Destroy the loop: free registered items and close the epoll fd
 *************************************************************************************************/
void event_loop_destroy(void)
{
    event_loop_clear_items();
    g_evloop.timeout_cb = NULL;
    if (g_evloop.epoll_fd >= 0)
    {
        close(g_evloop.epoll_fd);
        g_evloop.epoll_fd = -1;
    }
    g_evloop.initialized = false;
}

/**************************************************************************************************
 * @brief       Register a file descriptor and its callback with the event loop
 * @param[in]   fd        File descriptor to monitor
 * @param[in]   callback  Callback fired on events (must not be NULL)
 * @param[in]   userdata  User pointer passed back to the callback
 * @param[in]   events    Interested epoll event mask
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int event_loop_add_fd(int fd, event_loop_cb callback, void *userdata, uint32_t events)
{
    struct epoll_event ev;
    EventLoopItem *item = NULL;

    if (!g_evloop.initialized || fd < 0 || !callback)
    {
        return -1;
    }

    item = (EventLoopItem *)malloc(sizeof(EventLoopItem));
    if (!item)
    {
        return -1;
    }

    item->fd = fd;
    item->callback = callback;
    item->userdata = userdata;

    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.ptr = (void *)item;

    if (epoll_ctl(g_evloop.epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0)
    {
        free(item);
        return -1;
    }

    if (event_loop_register_item(item) != 0)
    {
        (void)epoll_ctl(g_evloop.epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        free(item);
        return -1;
    }

    return 0;
}

/**************************************************************************************************
 * @brief       Change the epoll event mask of a registered file descriptor
 * @param[in]   fd     File descriptor
 * @param[in]   events New event mask
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int event_loop_mod_fd(int fd, uint32_t events)
{
    struct epoll_event ev;
    EventLoopItem *item = NULL;

    if (!g_evloop.initialized || fd < 0)
    {
        return -1;
    }

    item = event_loop_find_item(fd);
    if (item == NULL)
    {
        return -1;
    }

    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.ptr = (void *)item;
    if (epoll_ctl(g_evloop.epoll_fd, EPOLL_CTL_MOD, fd, &ev) != 0)
    {
        return -1;
    }

    return 0;
}

/**************************************************************************************************
 * @brief       Remove a file descriptor from the event loop
 * @param[in]   fd File descriptor to remove
 * @return      0 on success, -1 on failure
 *************************************************************************************************/
int event_loop_del_fd(int fd)
{
    struct epoll_event ev;

    if (!g_evloop.initialized || fd < 0)
    {
        return -1;
    }

    memset(&ev, 0, sizeof(ev));
    if (epoll_ctl(g_evloop.epoll_fd, EPOLL_CTL_DEL, fd, &ev) != 0)
    {
        return -1;
    }

    event_loop_unregister_item(fd);
    return 0;
}

/**************************************************************************************************
 * @brief       Run the main event loop (blocking forever)
 * @return      Never returns on success, -1 on fatal error
 *************************************************************************************************/
int event_loop_run(void)
{
    int i = 0;

    if (!g_evloop.initialized)
    {
        return -1;
    }

    for (;;)
    {
        int nfds = epoll_wait(g_evloop.epoll_fd, g_evloop.events, EVENT_LOOP_MAX_EVENTS, EVENT_LOOP_TIMEOUT_MS);
        if (nfds < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            break;
        }

        if (nfds == 0)
        {
            event_loop_poll_registered();
            if (g_evloop.timeout_cb != NULL)
            {
                g_evloop.timeout_cb();
            }
            continue;
        }

        for (i = 0; i < nfds; i++)
        {
            EventLoopItem *item = (EventLoopItem *)g_evloop.events[i].data.ptr;

            if (item && item->callback)
            {
                item->callback(item->fd, g_evloop.events[i].events, item->userdata);
            }
        }
    }

    return -1;
}
