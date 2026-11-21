#include "protocol.h"
#include "mac_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <stdatomic.h>
#include <poll.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT_QUEUE_CAPACITY 32

/*
 * Shared frame with thread-safe reference count.
 * Enqueued into multiple output queues without copying.
 * Last sender to finish decrements refcount to 0 and frees it.
 */
typedef struct {
    frame_t frame;
    pthread_mutex_t lock;
    int refcount;
} shared_frame_t;

static shared_frame_t *shared_frame_create(const frame_t *frame) {
    shared_frame_t *sf = malloc(sizeof(*sf));
    if (!sf) {
        return NULL;
    }
    sf->frame = *frame;
    int err = pthread_mutex_init(&sf->lock, NULL);
    if (err != 0) {
        free(sf);
        return NULL;
    }
    /* Creator reference holds frame alive during candidate enqueue fan-out */
    sf->refcount = 1;
    return sf;
}

static void shared_frame_release(shared_frame_t *sf) {
    if (!sf) {
        return;
    }
    pthread_mutex_lock(&sf->lock);
    sf->refcount--;
    int remaining = sf->refcount;
    pthread_mutex_unlock(&sf->lock);

    if (remaining <= 0) {
        pthread_mutex_destroy(&sf->lock);
        free(sf);
    }
}

typedef enum {
    PORT_STATE_FREE = 0,
    PORT_STATE_ACTIVE,
    PORT_STATE_CLOSING,
    PORT_STATE_TERMINATED
} port_state_t;

typedef struct {
    int id;
    int fd;
    _Atomic int state;
    _Atomic bool rx_done;
    _Atomic bool tx_done;
    pthread_t rx_tid;
    pthread_t tx_tid;

    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_cond;

    shared_frame_t *queue[PORT_QUEUE_CAPACITY];
    int queue_head;
    int queue_tail;
    int queue_count;

    _Atomic uint64_t frames_in;
    _Atomic uint64_t frames_out;
    _Atomic uint64_t frames_dropped;
} port_t;

static _Atomic int g_running = 1;
static int g_num_ports = 0;
static port_t *g_ports = NULL;
static mac_table_t *g_mac_table = NULL;

static void handle_signal(int sig) {
    (void)sig;
    int saved_errno = errno;
    atomic_store_explicit(&g_running, 0, memory_order_relaxed);
    errno = saved_errno;
}

/*
 * Drains any remaining queued shared frames and decrements their refcounts.
 * Caller MUST hold port->queue_mutex.
 */
static void port_drain_queue_locked(port_t *port) {
    while (port->queue_count > 0) {
        shared_frame_t *sf = port->queue[port->queue_head];
        port->queue[port->queue_head] = NULL;
        port->queue_head = (port->queue_head + 1) % PORT_QUEUE_CAPACITY;
        port->queue_count--;
        shared_frame_release(sf);
    }
}

/*
 * Enqueues a shared_frame_t to the port's output queue without blocking.
 * If queue is full, increments frames_dropped and drops immediately (does not increment refcount).
 * If space available, increments sf->refcount and enqueues.
 */
static int port_enqueue(port_t *port, shared_frame_t *sf) {
    if (!port || !sf) {
        return -1;
    }

    pthread_mutex_lock(&port->queue_mutex);
    if (atomic_load_explicit(&port->state, memory_order_relaxed) != PORT_STATE_ACTIVE) {
        pthread_mutex_unlock(&port->queue_mutex);
        return -1;
    }

    if (port->queue_count >= PORT_QUEUE_CAPACITY) {
        atomic_fetch_add_explicit(&port->frames_dropped, 1, memory_order_relaxed);
        uint64_t drops = atomic_load_explicit(&port->frames_dropped, memory_order_relaxed);
        pthread_mutex_unlock(&port->queue_mutex);
        printf("[Switch] Port %d: Output queue full, dropped frame (total dropped: %lu)\n",
               port->id, (unsigned long)drops);
        fflush(stdout);
        return -1;
    }

    /* Reference count increments under sf->lock before queue insertion */
    pthread_mutex_lock(&sf->lock);
    sf->refcount++;
    pthread_mutex_unlock(&sf->lock);

    port->queue[port->queue_tail] = sf;
    port->queue_tail = (port->queue_tail + 1) % PORT_QUEUE_CAPACITY;
    port->queue_count++;
    pthread_cond_signal(&port->queue_cond);
    pthread_mutex_unlock(&port->queue_mutex);
    return 0;
}

/*
 * Sender thread: pops shared frames from queue and writes to socket.
 * Performs write_frame() outside queue_mutex.
 * Decrements shared frame refcount after sending.
 */
static void *port_tx_thread(void *arg) {
    port_t *port = (port_t *)arg;

    while (1) {
        shared_frame_t *sf = NULL;

        pthread_mutex_lock(&port->queue_mutex);
        while (port->queue_count == 0 &&
               atomic_load_explicit(&port->state, memory_order_relaxed) == PORT_STATE_ACTIVE &&
               atomic_load_explicit(&g_running, memory_order_relaxed)) {
            pthread_cond_wait(&port->queue_cond, &port->queue_mutex);
        }

        if (port->queue_count > 0) {
            sf = port->queue[port->queue_head];
            port->queue[port->queue_head] = NULL;
            port->queue_head = (port->queue_head + 1) % PORT_QUEUE_CAPACITY;
            port->queue_count--;
        } else if (atomic_load_explicit(&port->state, memory_order_relaxed) != PORT_STATE_ACTIVE ||
                   !atomic_load_explicit(&g_running, memory_order_relaxed)) {
            pthread_mutex_unlock(&port->queue_mutex);
            break;
        }
        pthread_mutex_unlock(&port->queue_mutex);

        if (sf) {
            if (write_frame(port->fd, &sf->frame) < 0) {
                printf("[Switch] Port %d: TX write failed: %s\n", port->id, strerror(errno));
                fflush(stdout);

                shared_frame_release(sf);

                pthread_mutex_lock(&port->queue_mutex);
                atomic_store_explicit(&port->state, PORT_STATE_CLOSING, memory_order_relaxed);
                pthread_mutex_unlock(&port->queue_mutex);
                shutdown(port->fd, SHUT_RDWR);
                break;
            } else {
                atomic_fetch_add_explicit(&port->frames_out, 1, memory_order_relaxed);
            }

            shared_frame_release(sf);
        }
    }

    atomic_store_explicit(&port->tx_done, true, memory_order_release);
    if (atomic_load_explicit(&port->rx_done, memory_order_acquire)) {
        atomic_store_explicit(&port->state, PORT_STATE_TERMINATED, memory_order_release);
    }
    return NULL;
}

/*
 * Receiver thread: reads frames from host socket.
 * Learns source MAC, checks port security, looks up destination in MAC table,
 * and performs unicast forwarding or broadcast flooding with refcounted shared frames.
 */
static void *port_rx_thread(void *arg) {
    port_t *port = (port_t *)arg;

    printf("[Switch] Port %d: Host connected\n", port->id);
    fflush(stdout);

    frame_t frame;
    while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
        int rc = read_frame(port->fd, &frame);
        if (rc == 0) {
            printf("[Switch] Port %d: Host disconnected cleanly\n", port->id);
            fflush(stdout);
            break;
        }
        if (rc < 0) {
            if (atomic_load_explicit(&g_running, memory_order_relaxed) &&
                atomic_load_explicit(&port->state, memory_order_relaxed) == PORT_STATE_ACTIVE) {
                printf("[Switch] Port %d: Read error / disconnect: %s\n",
                       port->id, strerror(errno));
                fflush(stdout);
            }
            break;
        }

        atomic_fetch_add_explicit(&port->frames_in, 1, memory_order_relaxed);

        char src_str[MAC_STR_LEN];
        char dst_str[MAC_STR_LEN];
        if (mac_to_str(frame.src, src_str, sizeof(src_str)) < 0) {
            strncpy(src_str, "??:??:??:??:??:??", sizeof(src_str));
        }
        if (mac_to_str(frame.dst, dst_str, sizeof(dst_str)) < 0) {
            strncpy(dst_str, "??:??:??:??:??:??", sizeof(dst_str));
        }

        printf("[Switch] Port %d: RX src=%s dst=%s vlan=%u prio=%u len=%u\n",
               port->id, src_str, dst_str, frame.vlan, frame.priority, frame.length);
        fflush(stdout);

        /* Learn source MAC and enforce port security */
        int learn_rc = mac_table_learn(g_mac_table, frame.src, port->id);
        if (learn_rc == MAC_TABLE_SECURITY_VIOLATION) {
            printf("[Switch] Port %d: Port security shutdown triggered!\n", port->id);
            fflush(stdout);
            shutdown(port->fd, SHUT_RDWR);
            break;
        }

        /* Forwarding logic: unicast vs flood */
        shared_frame_t *sf = shared_frame_create(&frame);
        if (!sf) {
            continue;
        }

        int dst_port = -1;
        if (!mac_is_broadcast(frame.dst)) {
            dst_port = mac_table_lookup(g_mac_table, frame.dst);
        }

        if (dst_port >= 0) {
            /* Known destination MAC */
            if (dst_port == port->id) {
                /* Filtered: destination sits on the incoming port */
                printf("[Switch] Port %d: Frame filtered (dest sits on same port)\n", port->id);
                fflush(stdout);
            } else {
                printf("[Switch] Port %d: Unicast forwarding to port %d\n", port->id, dst_port);
                fflush(stdout);
                port_enqueue(&g_ports[dst_port], sf);
            }
        } else {
            /* Broadcast or unknown unicast: flood to all other active ports */
            printf("[Switch] Port %d: Flooding frame (dst=%s)\n", port->id, dst_str);
            fflush(stdout);
            for (int i = 0; i < g_num_ports; i++) {
                if (i == port->id) {
                    continue;
                }
                port_enqueue(&g_ports[i], sf);
            }
        }

        /* Release creator reference; frame will be freed when all senders finish */
        shared_frame_release(sf);
    }

    /* Wake sender and mark port closing */
    pthread_mutex_lock(&port->queue_mutex);
    atomic_store_explicit(&port->state, PORT_STATE_CLOSING, memory_order_relaxed);
    pthread_cond_broadcast(&port->queue_cond);
    pthread_mutex_unlock(&port->queue_mutex);

    shutdown(port->fd, SHUT_RDWR);

    atomic_store_explicit(&port->rx_done, true, memory_order_release);
    if (atomic_load_explicit(&port->tx_done, memory_order_acquire)) {
        atomic_store_explicit(&port->state, PORT_STATE_TERMINATED, memory_order_release);
    }
    return NULL;
}

/*
 * Cleans up finished threads on ports where both rx and tx have completed.
 */
static void reap_terminated_ports(void) {
    for (int i = 0; i < g_num_ports; i++) {
        if (atomic_load_explicit(&g_ports[i].state, memory_order_relaxed) == PORT_STATE_TERMINATED) {
            int err = pthread_join(g_ports[i].rx_tid, NULL);
            if (err != 0) {
                fprintf(stderr, "pthread_join rx_tid failed: %s\n", strerror(err));
            }
            err = pthread_join(g_ports[i].tx_tid, NULL);
            if (err != 0) {
                fprintf(stderr, "pthread_join tx_tid failed: %s\n", strerror(err));
            }

            if (g_ports[i].fd >= 0) {
                if (close(g_ports[i].fd) < 0) {
                    perror("close port fd");
                }
                g_ports[i].fd = -1;
            }

            pthread_mutex_lock(&g_ports[i].queue_mutex);
            port_drain_queue_locked(&g_ports[i]);
            g_ports[i].queue_head = 0;
            g_ports[i].queue_tail = 0;
            g_ports[i].queue_count = 0;
            atomic_store_explicit(&g_ports[i].state, PORT_STATE_FREE, memory_order_relaxed);
            pthread_mutex_unlock(&g_ports[i].queue_mutex);

            /* Flush MAC entries learned on this port */
            mac_table_remove_port(g_mac_table, i);

            printf("[Switch] Port %d: Reaped and reset to free\n", i);
            fflush(stdout);
        }
    }
}

/*
 * Allocates a free port for a newly accepted connection.
 */
static int allocate_port(int client_fd) {
    reap_terminated_ports();

    for (int i = 0; i < g_num_ports; i++) {
        if (atomic_load_explicit(&g_ports[i].state, memory_order_relaxed) == PORT_STATE_FREE) {
            int sndbuf = 8192;
            if (setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf)) < 0) {
                perror("setsockopt(SO_SNDBUF)");
            }

            g_ports[i].fd = client_fd;
            g_ports[i].queue_head = 0;
            g_ports[i].queue_tail = 0;
            g_ports[i].queue_count = 0;
            atomic_store_explicit(&g_ports[i].rx_done, false, memory_order_relaxed);
            atomic_store_explicit(&g_ports[i].tx_done, false, memory_order_relaxed);
            atomic_store_explicit(&g_ports[i].state, PORT_STATE_ACTIVE, memory_order_relaxed);

            int err = pthread_create(&g_ports[i].tx_tid, NULL, port_tx_thread, &g_ports[i]);
            if (err != 0) {
                fprintf(stderr, "pthread_create tx_tid failed: %s\n", strerror(err));
                atomic_store_explicit(&g_ports[i].state, PORT_STATE_FREE, memory_order_relaxed);
                g_ports[i].fd = -1;
                return -1;
            }

            err = pthread_create(&g_ports[i].rx_tid, NULL, port_rx_thread, &g_ports[i]);
            if (err != 0) {
                fprintf(stderr, "pthread_create rx_tid failed: %s\n", strerror(err));
                pthread_mutex_lock(&g_ports[i].queue_mutex);
                atomic_store_explicit(&g_ports[i].state, PORT_STATE_CLOSING, memory_order_relaxed);
                pthread_cond_broadcast(&g_ports[i].queue_cond);
                pthread_mutex_unlock(&g_ports[i].queue_mutex);
                pthread_join(g_ports[i].tx_tid, NULL);
                atomic_store_explicit(&g_ports[i].state, PORT_STATE_FREE, memory_order_relaxed);
                g_ports[i].fd = -1;
                return -1;
            }

            return i;
        }
    }
    return -1;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <tcp_port> <num_ports>\n", argv[0]);
        return 1;
    }

    char *endptr = NULL;
    long port = strtol(argv[1], &endptr, 10);
    if (*endptr != '\0' || port <= 0 || port > 65535) {
        fprintf(stderr, "Error: Invalid tcp_port: %s\n", argv[1]);
        return 1;
    }

    long num_ports = strtol(argv[2], &endptr, 10);
    if (*endptr != '\0' || num_ports <= 0 || num_ports > MAX_SWITCH_PORTS) {
        fprintf(stderr, "Error: Invalid num_ports: %s (max %d)\n", argv[2], MAX_SWITCH_PORTS);
        return 1;
    }

    g_num_ports = (int)num_ports;
    g_ports = calloc((size_t)g_num_ports, sizeof(port_t));
    if (!g_ports) {
        perror("calloc g_ports");
        return 1;
    }

    for (int i = 0; i < g_num_ports; i++) {
        g_ports[i].id = i;
        g_ports[i].fd = -1;
        atomic_init(&g_ports[i].state, PORT_STATE_FREE);
        atomic_init(&g_ports[i].rx_done, false);
        atomic_init(&g_ports[i].tx_done, false);
        g_ports[i].queue_head = 0;
        g_ports[i].queue_tail = 0;
        g_ports[i].queue_count = 0;
        for (int q = 0; q < PORT_QUEUE_CAPACITY; q++) {
            g_ports[i].queue[q] = NULL;
        }
        atomic_init(&g_ports[i].frames_in, 0);
        atomic_init(&g_ports[i].frames_out, 0);
        atomic_init(&g_ports[i].frames_dropped, 0);

        int err = pthread_mutex_init(&g_ports[i].queue_mutex, NULL);
        if (err != 0) {
            fprintf(stderr, "pthread_mutex_init failed: %s\n", strerror(err));
            return 1;
        }
        err = pthread_cond_init(&g_ports[i].queue_cond, NULL);
        if (err != 0) {
            fprintf(stderr, "pthread_cond_init failed: %s\n", strerror(err));
            return 1;
        }
    }

    /* Initialize concurrent striped MAC table */
    g_mac_table = malloc(sizeof(mac_table_t));
    if (!g_mac_table) {
        perror("malloc mac_table");
        return 1;
    }
    if (mac_table_init(g_mac_table, 1024, 16, 30) < 0) {
        fprintf(stderr, "mac_table_init failed\n");
        return 1;
    }

    /* Ignore SIGPIPE so write() returns EPIPE on disconnected sockets */
    struct sigaction sa_pipe;
    memset(&sa_pipe, 0, sizeof(sa_pipe));
    sa_pipe.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa_pipe, NULL) < 0) {
        perror("sigaction(SIGPIPE)");
        return 1;
    }

    /* Handle SIGINT/SIGTERM for graceful teardown */
    struct sigaction sa_int;
    memset(&sa_int, 0, sizeof(sa_int));
    sa_int.sa_handler = handle_signal;
    if (sigaction(SIGINT, &sa_int, NULL) < 0) {
        perror("sigaction(SIGINT)");
        return 1;
    }
    if (sigaction(SIGTERM, &sa_int, NULL) < 0) {
        perror("sigaction(SIGTERM)");
        return 1;
    }

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR)");
        close(listen_fd);
        return 1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons((uint16_t)port);

    if (bind(listen_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, (int)num_ports) < 0) {
        perror("listen");
        close(listen_fd);
        return 1;
    }

    printf("[Switch] Started on port %ld with %ld max ports\n", port, num_ports);
    fflush(stdout);

    struct pollfd pfd;
    pfd.fd = listen_fd;
    pfd.events = POLLIN;

    while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
        int poll_rc = poll(&pfd, 1, 200);
        if (poll_rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("poll");
            break;
        }
        if (poll_rc == 0) {
            reap_terminated_ports();
            continue;
        }

        if (pfd.revents & POLLIN) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
            if (client_fd < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (!atomic_load_explicit(&g_running, memory_order_relaxed)) {
                    break;
                }
                perror("accept");
                break;
            }

            int port_id = allocate_port(client_fd);
            if (port_id < 0) {
                fprintf(stderr, "[Switch] All %ld ports occupied: rejecting connection\n", num_ports);
                close(client_fd);
            }
        }
    }

    printf("[Switch] Shutting down...\n");
    if (close(listen_fd) < 0) {
        perror("close listen_fd");
    }

    /* Unblock all active sender threads and signal closing */
    for (int i = 0; i < g_num_ports; i++) {
        int st = atomic_load_explicit(&g_ports[i].state, memory_order_relaxed);
        if (st == PORT_STATE_ACTIVE || st == PORT_STATE_CLOSING) {
            pthread_mutex_lock(&g_ports[i].queue_mutex);
            atomic_store_explicit(&g_ports[i].state, PORT_STATE_CLOSING, memory_order_relaxed);
            pthread_cond_broadcast(&g_ports[i].queue_cond);
            pthread_mutex_unlock(&g_ports[i].queue_mutex);
            if (g_ports[i].fd >= 0) {
                shutdown(g_ports[i].fd, SHUT_RDWR);
            }
        }
    }

    /* Join all threads and release resources */
    for (int i = 0; i < g_num_ports; i++) {
        int st = atomic_load_explicit(&g_ports[i].state, memory_order_relaxed);
        if (st != PORT_STATE_FREE) {
            int err = pthread_join(g_ports[i].rx_tid, NULL);
            if (err != 0) {
                fprintf(stderr, "pthread_join rx_tid failed: %s\n", strerror(err));
            }
            err = pthread_join(g_ports[i].tx_tid, NULL);
            if (err != 0) {
                fprintf(stderr, "pthread_join tx_tid failed: %s\n", strerror(err));
            }
            if (g_ports[i].fd >= 0) {
                if (close(g_ports[i].fd) < 0) {
                    perror("close port fd");
                }
                g_ports[i].fd = -1;
            }
        }

        pthread_mutex_lock(&g_ports[i].queue_mutex);
        port_drain_queue_locked(&g_ports[i]);
        pthread_mutex_unlock(&g_ports[i].queue_mutex);

        pthread_mutex_destroy(&g_ports[i].queue_mutex);
        pthread_cond_destroy(&g_ports[i].queue_cond);
    }

    mac_table_destroy(g_mac_table);
    free(g_mac_table);
    free(g_ports);
    return 0;
}
