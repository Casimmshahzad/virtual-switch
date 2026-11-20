#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <stdatomic.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdbool.h>

#define PORT_QUEUE_CAPACITY 32

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

    frame_t queue[PORT_QUEUE_CAPACITY];
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

static void handle_signal(int sig) {
    (void)sig;
    int saved_errno = errno;
    atomic_store_explicit(&g_running, 0, memory_order_relaxed);
    errno = saved_errno;
}

/*
 * Enqueues a frame to the port's output queue without blocking.
 * If queue is full, drops the frame immediately and increments frames_dropped.
 */
static int port_enqueue(port_t *port, const frame_t *frame) {
    if (!port || !frame) {
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

    port->queue[port->queue_tail] = *frame;
    port->queue_tail = (port->queue_tail + 1) % PORT_QUEUE_CAPACITY;
    port->queue_count++;
    pthread_cond_signal(&port->queue_cond);
    pthread_mutex_unlock(&port->queue_mutex);
    return 0;
}

/*
 * Sender thread: pops frames from queue and writes to socket.
 * Waits on condition variable when empty.
 * Socket writes are strictly performed outside queue_mutex to prevent HOL blocking.
 */
static void *port_tx_thread(void *arg) {
    port_t *port = (port_t *)arg;

    while (1) {
        frame_t frame;
        bool has_frame = false;

        pthread_mutex_lock(&port->queue_mutex);
        while (port->queue_count == 0 &&
               atomic_load_explicit(&port->state, memory_order_relaxed) == PORT_STATE_ACTIVE &&
               atomic_load_explicit(&g_running, memory_order_relaxed)) {
            pthread_cond_wait(&port->queue_cond, &port->queue_mutex);
        }

        if (port->queue_count > 0) {
            frame = port->queue[port->queue_head];
            port->queue_head = (port->queue_head + 1) % PORT_QUEUE_CAPACITY;
            port->queue_count--;
            has_frame = true;
        } else if (atomic_load_explicit(&port->state, memory_order_relaxed) != PORT_STATE_ACTIVE ||
                   !atomic_load_explicit(&g_running, memory_order_relaxed)) {
            pthread_mutex_unlock(&port->queue_mutex);
            break;
        }
        pthread_mutex_unlock(&port->queue_mutex);

        if (has_frame) {
            /* Performed outside mutex so slow host never blocks other ports or receivers */
            if (write_frame(port->fd, &frame) < 0) {
                printf("[Switch] Port %d: TX write failed: %s\n", port->id, strerror(errno));
                fflush(stdout);

                pthread_mutex_lock(&port->queue_mutex);
                atomic_store_explicit(&port->state, PORT_STATE_CLOSING, memory_order_relaxed);
                pthread_mutex_unlock(&port->queue_mutex);
                shutdown(port->fd, SHUT_RDWR);
                break;
            } else {
                atomic_fetch_add_explicit(&port->frames_out, 1, memory_order_relaxed);
            }
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
 * Floods incoming frames to all other active ports.
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

        /* Flood incoming frame to all other active ports */
        for (int i = 0; i < g_num_ports; i++) {
            if (i == port->id) {
                continue;
            }
            port_enqueue(&g_ports[i], &frame);
        }
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
            g_ports[i].queue_head = 0;
            g_ports[i].queue_tail = 0;
            g_ports[i].queue_count = 0;
            atomic_store_explicit(&g_ports[i].state, PORT_STATE_FREE, memory_order_relaxed);
            pthread_mutex_unlock(&g_ports[i].queue_mutex);

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
            /* Bound socket send buffer so queue backpressure triggers promptly on slow hosts */
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
    if (*endptr != '\0' || num_ports <= 0) {
        fprintf(stderr, "Error: Invalid num_ports: %s\n", argv[2]);
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
        pthread_mutex_destroy(&g_ports[i].queue_mutex);
        pthread_cond_destroy(&g_ports[i].queue_cond);
    }

    free(g_ports);
    return 0;
}
