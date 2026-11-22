#include "protocol.h"
#include "mac_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <poll.h>
#include <stdbool.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define NUM_PRIORITIES       3
#define PORT_QUEUE_CAPACITY  32
#define DEFAULT_RATE_LIMIT   100

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
    shared_frame_t *sf;
    struct timespec enqueue_time;
} queue_entry_t;

typedef struct {
    int id;
    _Atomic int fd;
    _Atomic int state;
    _Atomic bool rx_done;
    _Atomic bool tx_done;
    pthread_t rx_tid;
    pthread_t tx_tid;

    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_cond;

    /* 3 Bounded Priority Queues (0=High, 1=Normal, 2=Low) */
    queue_entry_t queues[NUM_PRIORITIES][PORT_QUEUE_CAPACITY];
    int q_head[NUM_PRIORITIES];
    int q_tail[NUM_PRIORITIES];
    int q_count[NUM_PRIORITIES];

    /* WRR scheduling state (5:3:1) */
    int wrr_current_q;
    int wrr_weight_left;

    /* Token bucket rate limiter */
    int rate_limit;      /* frames per second */
    int tokens;          /* available token balance */
    int token_ms_accum;  /* accumulated milliseconds for fractional token refill */

    /* VLAN and Port Mirroring (Milestone 6) */
    _Atomic uint16_t vlan_id;  /* Assigned VLAN ID (default 1) */
    _Atomic bool is_trunk;     /* True if port is a trunk carrying all VLANs */
    _Atomic int mirror_dest;   /* Target sniffer port ID, or -1 if disabled */

    _Atomic uint64_t frames_in;
    _Atomic uint64_t frames_out;
    _Atomic uint64_t frames_dropped;
} port_t;

static _Atomic int g_running = 1;
static int g_num_ports = 0;
static port_t *g_ports = NULL;
static mac_table_t *g_mac_table = NULL;
static sem_t g_port_sem;
static _Atomic int g_listen_fd = -1;

/* Rate limiter timer thread state */
static _Atomic int g_default_rate_limit = DEFAULT_RATE_LIMIT;
static pthread_t g_rate_timer_tid;
static pthread_mutex_t g_rate_timer_mutex;
static pthread_cond_t g_rate_timer_cond;

/*
 * Drains all queued frames across all 3 priority queues and releases refcounts.
 * Caller MUST hold port->queue_mutex.
 */
static void port_drain_queue_locked(port_t *port) {
    for (int p = 0; p < NUM_PRIORITIES; p++) {
        while (port->q_count[p] > 0) {
            shared_frame_t *sf = port->queues[p][port->q_head[p]].sf;
            port->queues[p][port->q_head[p]].sf = NULL;
            port->q_head[p] = (port->q_head[p] + 1) % PORT_QUEUE_CAPACITY;
            port->q_count[p]--;
            shared_frame_release(sf);
        }
    }
}

/*
 * Enqueues a shared_frame_t into the appropriate priority queue (0=high, 1=normal, 2=low).
 * If the priority queue is full, increments frames_dropped and drops immediately.
 */
static int port_enqueue(port_t *port, shared_frame_t *sf) {
    if (!port || !sf) {
        return -1;
    }

    int prio = sf->frame.priority;
    if (prio < 0 || prio >= NUM_PRIORITIES) {
        prio = PRIORITY_NORMAL;
    }

    pthread_mutex_lock(&port->queue_mutex);
    if (atomic_load_explicit(&port->state, memory_order_relaxed) != PORT_STATE_ACTIVE) {
        pthread_mutex_unlock(&port->queue_mutex);
        return -1;
    }

    if (port->q_count[prio] >= PORT_QUEUE_CAPACITY) {
        atomic_fetch_add_explicit(&port->frames_dropped, 1, memory_order_relaxed);
        uint64_t drops = atomic_load_explicit(&port->frames_dropped, memory_order_relaxed);
        pthread_mutex_unlock(&port->queue_mutex);
        printf("[Switch] Port %d: Priority %d queue full, dropped frame (total dropped: %lu)\n",
               port->id, prio, (unsigned long)drops);
        fflush(stdout);
        return -1;
    }

    /* Increment refcount under sf->lock before queue insertion */
    pthread_mutex_lock(&sf->lock);
    sf->refcount++;
    pthread_mutex_unlock(&sf->lock);

    int tail = port->q_tail[prio];
    port->queues[prio][tail].sf = sf;
    clock_gettime(CLOCK_MONOTONIC, &port->queues[prio][tail].enqueue_time);
    port->q_tail[prio] = (tail + 1) % PORT_QUEUE_CAPACITY;
    port->q_count[prio]++;

    pthread_cond_signal(&port->queue_cond);
    pthread_mutex_unlock(&port->queue_mutex);
    return 0;
}

/*
 * Frame aging: promotes Low (2) -> Normal (1) and Normal (1) -> High (0)
 * if frames have waited > 200ms in queue to prevent starvation.
 * Caller MUST hold port->queue_mutex.
 */
static void port_age_queues_locked(port_t *port) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    /* Check Low priority queue (2) for frames waiting > 200ms */
    while (port->q_count[PRIORITY_LOW] > 0) {
        int head = port->q_head[PRIORITY_LOW];
        long elapsed_ms = (now.tv_sec - port->queues[PRIORITY_LOW][head].enqueue_time.tv_sec) * 1000 +
                          (now.tv_nsec - port->queues[PRIORITY_LOW][head].enqueue_time.tv_nsec) / 1000000;
        if (elapsed_ms >= 200) {
            if (port->q_count[PRIORITY_NORMAL] < PORT_QUEUE_CAPACITY) {
                queue_entry_t entry = port->queues[PRIORITY_LOW][head];
                port->queues[PRIORITY_LOW][head].sf = NULL;
                port->q_head[PRIORITY_LOW] = (head + 1) % PORT_QUEUE_CAPACITY;
                port->q_count[PRIORITY_LOW]--;

                entry.enqueue_time = now; /* Reset wait timer for newly promoted queue */
                int n_tail = port->q_tail[PRIORITY_NORMAL];
                port->queues[PRIORITY_NORMAL][n_tail] = entry;
                port->q_tail[PRIORITY_NORMAL] = (n_tail + 1) % PORT_QUEUE_CAPACITY;
                port->q_count[PRIORITY_NORMAL]++;
                printf("[QoS] Port %d: Aged frame (wait %ld ms) promoted Low -> Normal\n",
                       port->id, elapsed_ms);
                fflush(stdout);
            } else {
                break;
            }
        } else {
            break;
        }
    }

    /* Check Normal priority queue (1) for frames waiting > 200ms */
    while (port->q_count[PRIORITY_NORMAL] > 0) {
        int head = port->q_head[PRIORITY_NORMAL];
        long elapsed_ms = (now.tv_sec - port->queues[PRIORITY_NORMAL][head].enqueue_time.tv_sec) * 1000 +
                          (now.tv_nsec - port->queues[PRIORITY_NORMAL][head].enqueue_time.tv_nsec) / 1000000;
        if (elapsed_ms >= 200) {
            if (port->q_count[PRIORITY_HIGH] < PORT_QUEUE_CAPACITY) {
                queue_entry_t entry = port->queues[PRIORITY_NORMAL][head];
                port->queues[PRIORITY_NORMAL][head].sf = NULL;
                port->q_head[PRIORITY_NORMAL] = (head + 1) % PORT_QUEUE_CAPACITY;
                port->q_count[PRIORITY_NORMAL]--;

                entry.enqueue_time = now; /* Reset wait timer for newly promoted queue */
                int h_tail = port->q_tail[PRIORITY_HIGH];
                port->queues[PRIORITY_HIGH][h_tail] = entry;
                port->q_tail[PRIORITY_HIGH] = (h_tail + 1) % PORT_QUEUE_CAPACITY;
                port->q_count[PRIORITY_HIGH]++;
                printf("[QoS] Port %d: Aged frame (wait %ld ms) promoted Normal -> High\n",
                       port->id, elapsed_ms);
                fflush(stdout);
            } else {
                break;
            }
        } else {
            break;
        }
    }
}

/*
 * Picks next frame using Weighted Round Robin (5:3:1) with aging.
 * Caller MUST hold port->queue_mutex.
 */
static shared_frame_t *port_pick_next_frame_locked(port_t *port) {
    port_age_queues_locked(port);

    int total = port->q_count[0] + port->q_count[1] + port->q_count[2];
    if (total == 0) {
        return NULL;
    }

    const int weights[NUM_PRIORITIES] = {5, 3, 1};

    for (int attempts = 0; attempts < NUM_PRIORITIES * 2; attempts++) {
        int q = port->wrr_current_q;
        if (port->q_count[q] > 0 && port->wrr_weight_left > 0) {
            int head = port->q_head[q];
            shared_frame_t *sf = port->queues[q][head].sf;
            port->queues[q][head].sf = NULL;
            port->q_head[q] = (head + 1) % PORT_QUEUE_CAPACITY;
            port->q_count[q]--;
            port->wrr_weight_left--;

            if (port->wrr_weight_left <= 0) {
                port->wrr_current_q = (port->wrr_current_q + 1) % NUM_PRIORITIES;
                port->wrr_weight_left = weights[port->wrr_current_q];
            }
            return sf;
        }

        port->wrr_current_q = (port->wrr_current_q + 1) % NUM_PRIORITIES;
        port->wrr_weight_left = weights[port->wrr_current_q];
    }

    /* Fallback to any non-empty queue in priority order */
    for (int q = 0; q < NUM_PRIORITIES; q++) {
        if (port->q_count[q] > 0) {
            int head = port->q_head[q];
            shared_frame_t *sf = port->queues[q][head].sf;
            port->queues[q][head].sf = NULL;
            port->q_head[q] = (head + 1) % PORT_QUEUE_CAPACITY;
            port->q_count[q]--;
            return sf;
        }
    }
    return NULL;
}

/*
 * Sender thread: schedules frames via WRR (5:3:1) and rate limits via Token Bucket.
 * Waits on pthread_cond_wait when empty, and pthread_cond_timedwait when out of tokens.
 */
static void *port_tx_thread(void *arg) {
    port_t *port = (port_t *)arg;

    while (1) {
        shared_frame_t *sf = NULL;

        pthread_mutex_lock(&port->queue_mutex);
        while (1) {
            if (!atomic_load_explicit(&g_running, memory_order_relaxed) ||
                atomic_load_explicit(&port->state, memory_order_relaxed) != PORT_STATE_ACTIVE) {
                break;
            }

            int total_queued = port->q_count[0] + port->q_count[1] + port->q_count[2];
            if (total_queued == 0) {
                /* Queue is empty: wait for frames */
                pthread_cond_wait(&port->queue_cond, &port->queue_mutex);
                continue;
            }

            /* Token bucket rate limiter check */
            if (port->tokens <= 0) {
                /* Out of tokens: wait for timer refill via pthread_cond_timedwait */
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_nsec += 50000000L; /* 50ms wait */
                if (ts.tv_nsec >= 1000000000L) {
                    ts.tv_sec += 1;
                    ts.tv_nsec -= 1000000000L;
                }
                pthread_cond_timedwait(&port->queue_cond, &port->queue_mutex, &ts);
                continue;
            }

            /* Frames and tokens are both available: pick next frame with WRR */
            sf = port_pick_next_frame_locked(port);
            if (sf) {
                port->tokens--;
            }
            break;
        }

        if (!sf) {
            if (!atomic_load_explicit(&g_running, memory_order_relaxed) ||
                atomic_load_explicit(&port->state, memory_order_relaxed) != PORT_STATE_ACTIVE) {
                pthread_mutex_unlock(&port->queue_mutex);
                break;
            }
        }
        pthread_mutex_unlock(&port->queue_mutex);

        if (sf) {
            int fd = atomic_load_explicit(&port->fd, memory_order_relaxed);
            if (fd >= 0 && write_frame(fd, &sf->frame) < 0) {
                printf("[Switch] Port %d: TX write failed: %s\n", port->id, strerror(errno));
                fflush(stdout);

                shared_frame_release(sf);

                pthread_mutex_lock(&port->queue_mutex);
                atomic_store_explicit(&port->state, PORT_STATE_CLOSING, memory_order_relaxed);
                int cfd = atomic_load_explicit(&port->fd, memory_order_relaxed);
                pthread_mutex_unlock(&port->queue_mutex);
                if (cfd >= 0) {
                    shutdown(cfd, SHUT_RDWR);
                }
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
        sem_post(&g_port_sem);
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
        int fd = atomic_load_explicit(&port->fd, memory_order_relaxed);
        if (fd < 0) {
            break;
        }
        int rc = read_frame(fd, &frame);
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

        bool is_trunk = atomic_load_explicit(&port->is_trunk, memory_order_relaxed);
        uint16_t port_vlan = atomic_load_explicit(&port->vlan_id, memory_order_relaxed);
        if (!is_trunk) {
            /* Access port: frames belong to the port's assigned VLAN */
            frame.vlan = port_vlan;
        } else {
            /* Trunk port: preserves frame.vlan (default to 1 if 0) */
            if (frame.vlan == 0) {
                frame.vlan = 1;
            }
        }

        char src_str[MAC_STR_LEN];
        char dst_str[MAC_STR_LEN];
        if (mac_to_str(frame.src, src_str, sizeof(src_str)) < 0) {
            strncpy(src_str, "??:??:??:??:??:??", sizeof(src_str));
        }
        if (mac_to_str(frame.dst, dst_str, sizeof(dst_str)) < 0) {
            strncpy(dst_str, "??:??:??:??:??:??", sizeof(dst_str));
        }

        printf("[Switch] Port %d (%s%u): RX src=%s dst=%s vlan=%u prio=%u len=%u\n",
               port->id, is_trunk ? "trunk" : "vlan", is_trunk ? 0 : port_vlan,
               src_str, dst_str, frame.vlan, frame.priority, frame.length);
        fflush(stdout);

        /* Learn source MAC and enforce port security per VLAN */
        int learn_rc = mac_table_learn(g_mac_table, frame.src, frame.vlan, port->id);
        if (learn_rc == MAC_TABLE_SECURITY_VIOLATION) {
            printf("[Switch] Port %d: Port security shutdown triggered!\n", port->id);
            fflush(stdout);
            int sfd = atomic_load_explicit(&port->fd, memory_order_relaxed);
            if (sfd >= 0) {
                shutdown(sfd, SHUT_RDWR);
            }
            break;
        }

        /* Forwarding logic: unicast vs flood */
        shared_frame_t *sf = shared_frame_create(&frame);
        if (!sf) {
            continue;
        }

        /* Port Mirroring: clone ingress frame to sniffer port without blocking */
        int sniffer = atomic_load_explicit(&port->mirror_dest, memory_order_relaxed);
        if (sniffer >= 0 && sniffer < g_num_ports && sniffer != port->id) {
            if (atomic_load_explicit(&g_ports[sniffer].state, memory_order_relaxed) == PORT_STATE_ACTIVE) {
                if (port_enqueue(&g_ports[sniffer], sf) == 0) {
                    printf("[Mirror] Cloned frame from port %d to sniffer port %d\n", port->id, sniffer);
                    fflush(stdout);
                }
            }
        }

        int dst_port = -1;
        if (!mac_is_broadcast(frame.dst)) {
            dst_port = mac_table_lookup(g_mac_table, frame.dst, frame.vlan);
        }

        if (dst_port >= 0) {
            /* Verify that destination port can carry this VLAN */
            bool dst_is_trunk = atomic_load_explicit(&g_ports[dst_port].is_trunk, memory_order_relaxed);
            uint16_t dst_vlan = atomic_load_explicit(&g_ports[dst_port].vlan_id, memory_order_relaxed);
            if (!dst_is_trunk && dst_vlan != frame.vlan) {
                /* Destination port is on another VLAN: drop/filter */
                dst_port = -1;
            }
        }

        if (dst_port >= 0) {
            /* Known destination MAC on same VLAN */
            if (dst_port == port->id) {
                /* Filtered: destination sits on the incoming port */
                printf("[Switch] Port %d: Frame filtered (dest sits on same port)\n", port->id);
                fflush(stdout);
            } else {
                printf("[Switch] Port %d: Unicast forwarding to port %d (vlan=%u)\n",
                       port->id, dst_port, frame.vlan);
                fflush(stdout);
                if (dst_port != sniffer) {
                    port_enqueue(&g_ports[dst_port], sf);
                }
            }
        } else {
            /* Broadcast or unknown destination: flood strictly within VLAN and to trunk ports */
            printf("[Switch] Port %d: Flooding frame (dst=%s, vlan=%u)\n",
                   port->id, dst_str, frame.vlan);
            fflush(stdout);
            for (int i = 0; i < g_num_ports; i++) {
                if (i == port->id || i == sniffer) {
                    continue;
                }
                if (atomic_load_explicit(&g_ports[i].state, memory_order_relaxed) != PORT_STATE_ACTIVE) {
                    continue;
                }
                bool i_trunk = atomic_load_explicit(&g_ports[i].is_trunk, memory_order_relaxed);
                uint16_t i_vlan = atomic_load_explicit(&g_ports[i].vlan_id, memory_order_relaxed);
                if (i_trunk || i_vlan == frame.vlan) {
                    port_enqueue(&g_ports[i], sf);
                }
            }
        }

        /* Release creator reference; frame is freed when all senders finish */
        shared_frame_release(sf);
    }

    /* Wake sender and mark port closing */
    pthread_mutex_lock(&port->queue_mutex);
    atomic_store_explicit(&port->state, PORT_STATE_CLOSING, memory_order_relaxed);
    pthread_cond_broadcast(&port->queue_cond);
    int cfd = atomic_load_explicit(&port->fd, memory_order_relaxed);
    pthread_mutex_unlock(&port->queue_mutex);

    if (cfd >= 0) {
        shutdown(cfd, SHUT_RDWR);
    }

    atomic_store_explicit(&port->rx_done, true, memory_order_release);
    if (atomic_load_explicit(&port->tx_done, memory_order_acquire)) {
        atomic_store_explicit(&port->state, PORT_STATE_TERMINATED, memory_order_release);
        sem_post(&g_port_sem);
    }
    return NULL;
}

/*
 * Dedicated token bucket timer thread: refills tokens periodically.
 */
static void *rate_limiter_thread(void *arg) {
    (void)arg;

    struct timespec last_tick;
    clock_gettime(CLOCK_MONOTONIC, &last_tick);

    while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 100000000L; /* 100 ms refill tick */
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000L;
        }

        pthread_mutex_lock(&g_rate_timer_mutex);
        while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
            int rc = pthread_cond_timedwait(&g_rate_timer_cond, &g_rate_timer_mutex, &ts);
            if (rc == ETIMEDOUT) {
                break;
            }
        }
        pthread_mutex_unlock(&g_rate_timer_mutex);

        if (!atomic_load_explicit(&g_running, memory_order_relaxed)) {
            break;
        }

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        long elapsed_ms = (now.tv_sec - last_tick.tv_sec) * 1000 +
                          (now.tv_nsec - last_tick.tv_nsec) / 1000000;
        if (elapsed_ms <= 0) {
            elapsed_ms = 1;
        }
        last_tick = now;

        /* Refill tokens for all active ports */
        for (int i = 0; i < g_num_ports; i++) {
            pthread_mutex_lock(&g_ports[i].queue_mutex);
            if (atomic_load_explicit(&g_ports[i].state, memory_order_relaxed) == PORT_STATE_ACTIVE) {
                int rate = g_ports[i].rate_limit;
                if (rate <= 0) {
                    rate = 100;
                }

                g_ports[i].token_ms_accum += (int)elapsed_ms;
                int new_tokens = (g_ports[i].token_ms_accum * rate) / 1000;
                if (new_tokens > 0) {
                    g_ports[i].tokens += new_tokens;
                    g_ports[i].token_ms_accum -= (new_tokens * 1000) / rate;
                    if (g_ports[i].tokens > rate) {
                        g_ports[i].tokens = rate;
                    }
                    pthread_cond_broadcast(&g_ports[i].queue_cond);
                }
            }
            pthread_mutex_unlock(&g_ports[i].queue_mutex);
        }
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

            pthread_mutex_lock(&g_ports[i].queue_mutex);
            int fd = atomic_exchange_explicit(&g_ports[i].fd, -1, memory_order_relaxed);
            port_drain_queue_locked(&g_ports[i]);
            for (int p = 0; p < NUM_PRIORITIES; p++) {
                g_ports[i].q_head[p] = 0;
                g_ports[i].q_tail[p] = 0;
                g_ports[i].q_count[p] = 0;
            }
            g_ports[i].wrr_current_q = 0;
            g_ports[i].wrr_weight_left = 5;
            g_ports[i].tokens = g_ports[i].rate_limit;
            g_ports[i].token_ms_accum = 0;
            atomic_store_explicit(&g_ports[i].state, PORT_STATE_FREE, memory_order_relaxed);
            pthread_mutex_unlock(&g_ports[i].queue_mutex);

            if (fd >= 0) {
                if (close(fd) < 0) {
                    perror("close port fd");
                }
            }

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

            atomic_store_explicit(&g_ports[i].fd, client_fd, memory_order_relaxed);
            for (int p = 0; p < NUM_PRIORITIES; p++) {
                g_ports[i].q_head[p] = 0;
                g_ports[i].q_tail[p] = 0;
                g_ports[i].q_count[p] = 0;
            }
            g_ports[i].wrr_current_q = 0;
            g_ports[i].wrr_weight_left = 5;
            g_ports[i].rate_limit = atomic_load_explicit(&g_default_rate_limit, memory_order_relaxed);
            g_ports[i].tokens = g_ports[i].rate_limit;
            g_ports[i].token_ms_accum = 0;

            atomic_store_explicit(&g_ports[i].rx_done, false, memory_order_relaxed);
            atomic_store_explicit(&g_ports[i].tx_done, false, memory_order_relaxed);
            atomic_store_explicit(&g_ports[i].state, PORT_STATE_ACTIVE, memory_order_relaxed);

            int err = pthread_create(&g_ports[i].tx_tid, NULL, port_tx_thread, &g_ports[i]);
            if (err != 0) {
                fprintf(stderr, "pthread_create tx_tid failed: %s\n", strerror(err));
                atomic_store_explicit(&g_ports[i].state, PORT_STATE_FREE, memory_order_relaxed);
                atomic_store_explicit(&g_ports[i].fd, -1, memory_order_relaxed);
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
                atomic_store_explicit(&g_ports[i].fd, -1, memory_order_relaxed);
                return -1;
            }

            return i;
        }
    }
    return -1;
}

/*
 * Reloads switch configuration from config file on SIGHUP.
 */
static void reload_config(const char *filename) {
    FILE *f = fopen(filename, "r");
    if (!f) {
        printf("[Config] Note: %s not found (retaining current settings)\n", filename);
        fflush(stdout);
        return;
    }
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || *p == '\n' || *p == '\0') continue;

        char key[64], val[64];
        int p_id, v_id;
        int t_port, t_val = 1;
        int m_src, m_dst;

        if (sscanf(p, "vlan %d = %d", &p_id, &v_id) == 2 ||
            sscanf(p, "vlan %d %d", &p_id, &v_id) == 2 ||
            sscanf(p, "port_%d_vlan = %d", &p_id, &v_id) == 2 ||
            sscanf(p, "port_%d_vlan=%d", &p_id, &v_id) == 2) {
            if (p_id >= 0 && p_id < g_num_ports && v_id >= 1 && v_id <= 4094) {
                atomic_store_explicit(&g_ports[p_id].vlan_id, (uint16_t)v_id, memory_order_relaxed);
                printf("[Config] Reloaded: port %d VLAN = %d\n", p_id, v_id);
            }
        } else if (sscanf(p, "trunk %d = %d", &t_port, &t_val) >= 1 ||
                   sscanf(p, "trunk %d %d", &t_port, &t_val) >= 1 ||
                   sscanf(p, "trunk_port = %d", &t_port) == 1 ||
                   sscanf(p, "port_%d_trunk = %d", &t_port, &t_val) >= 1) {
            if (t_port >= 0 && t_port < g_num_ports) {
                atomic_store_explicit(&g_ports[t_port].is_trunk, (t_val != 0), memory_order_relaxed);
                printf("[Config] Reloaded: port %d trunk = %s\n", t_port, (t_val != 0) ? "enabled" : "disabled");
            }
        } else if (sscanf(p, "mirror %d = %d", &m_src, &m_dst) == 2 ||
                   sscanf(p, "mirror %d %d", &m_src, &m_dst) == 2 ||
                   sscanf(p, "mirror_%d = %d", &m_src, &m_dst) == 2) {
            if (m_src >= 0 && m_src < g_num_ports && m_dst >= 0 && m_dst < g_num_ports && m_src != m_dst) {
                atomic_store_explicit(&g_ports[m_src].mirror_dest, m_dst, memory_order_relaxed);
                printf("[Config] Reloaded: mirroring port %d -> sniffer port %d\n", m_src, m_dst);
            }
        } else if (sscanf(p, "nomirror %d", &m_src) == 1 || sscanf(p, "unmirror %d", &m_src) == 1) {
            if (m_src >= 0 && m_src < g_num_ports) {
                atomic_store_explicit(&g_ports[m_src].mirror_dest, -1, memory_order_relaxed);
                printf("[Config] Reloaded: disabled mirroring for port %d\n", m_src);
            }
        } else if (sscanf(p, "%63[^= ] = %63s", key, val) == 2 ||
                   sscanf(p, "%63[^= ]=%63s", key, val) == 2) {
            if (strcmp(key, "aging_time") == 0) {
                int t = atoi(val);
                if (t > 0 && g_mac_table) {
                    atomic_store_explicit(&g_mac_table->aging_seconds, t, memory_order_relaxed);
                    printf("[Config] Reloaded: aging_time = %d seconds\n", t);
                }
            } else if (strcmp(key, "mac_limit") == 0) {
                int m = atoi(val);
                if (m > 0 && g_mac_table) {
                    atomic_store_explicit(&g_mac_table->max_per_port, m, memory_order_relaxed);
                    printf("[Config] Reloaded: mac_limit = %d per port\n", m);
                }
            } else if (strcmp(key, "rate_limit") == 0) {
                int r = atoi(val);
                if (r > 0) {
                    atomic_store_explicit(&g_default_rate_limit, r, memory_order_relaxed);
                    for (int i = 0; i < g_num_ports; i++) {
                        pthread_mutex_lock(&g_ports[i].queue_mutex);
                        g_ports[i].rate_limit = r;
                        pthread_mutex_unlock(&g_ports[i].queue_mutex);
                    }
                    printf("[Config] Reloaded: rate_limit = %d frames/sec\n", r);
                }
            }
        }
    }
    fclose(f);
    printf("[Config] %s reloaded successfully\n", filename);
    fflush(stdout);
}

/*
 * Dedicated signal thread handling SIGINT, SIGTERM, SIGHUP, and SIGUSR1 via sigwait.
 * All other threads block these signals.
 */
static void *signal_thread(void *arg) {
    (void)arg;
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGHUP);
    sigaddset(&set, SIGUSR1);

    while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
        int sig = 0;
        int err = sigwait(&set, &sig);
        if (err != 0) {
            if (err == EINTR) continue;
            break;
        }

        if (sig == SIGINT || sig == SIGTERM) {
            printf("\n[Switch] Signal %d (%s) received: initiating clean shutdown\n",
                   sig, (sig == SIGINT) ? "SIGINT" : "SIGTERM");
            fflush(stdout);

            atomic_store_explicit(&g_running, 0, memory_order_relaxed);

            /* Unblock any connection waiting on sem_timedwait */
            sem_post(&g_port_sem);

            /* Unblock rate limiter timer thread */
            pthread_mutex_lock(&g_rate_timer_mutex);
            pthread_cond_broadcast(&g_rate_timer_cond);
            pthread_mutex_unlock(&g_rate_timer_mutex);

            /* Unblock aging thread */
            if (g_mac_table) {
                atomic_store_explicit(&g_mac_table->running, false, memory_order_relaxed);
                pthread_mutex_lock(&g_mac_table->aging_mutex);
                pthread_cond_broadcast(&g_mac_table->aging_cond);
                pthread_mutex_unlock(&g_mac_table->aging_mutex);
            }

            /* Unblock active port queues and sockets */
            for (int i = 0; i < g_num_ports; i++) {
                int st = atomic_load_explicit(&g_ports[i].state, memory_order_relaxed);
                if (st == PORT_STATE_ACTIVE) {
                    pthread_mutex_lock(&g_ports[i].queue_mutex);
                    atomic_store_explicit(&g_ports[i].state, PORT_STATE_CLOSING, memory_order_relaxed);
                    pthread_cond_broadcast(&g_ports[i].queue_cond);
                    int fd = atomic_load_explicit(&g_ports[i].fd, memory_order_relaxed);
                    pthread_mutex_unlock(&g_ports[i].queue_mutex);
                    if (fd >= 0) {
                        shutdown(fd, SHUT_RDWR);
                    }
                }
            }
            break;
        } else if (sig == SIGHUP) {
            printf("[Switch] SIGHUP received: reloading switch.conf\n");
            fflush(stdout);
            reload_config("switch.conf");
        } else if (sig == SIGUSR1) {
            printf("[Switch] SIGUSR1 received: dumping MAC table to mac_dump.txt\n");
            fflush(stdout);
            if (g_mac_table) {
                mac_table_dump(g_mac_table, "mac_dump.txt");
            }
        }
    }
    return NULL;
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

    /*
     * Block signals in main thread before spawning any threads.
     * All spawned threads inherit this signal mask.
     */
    sigset_t sig_mask;
    sigemptyset(&sig_mask);
    sigaddset(&sig_mask, SIGINT);
    sigaddset(&sig_mask, SIGTERM);
    sigaddset(&sig_mask, SIGHUP);
    sigaddset(&sig_mask, SIGUSR1);
    if (pthread_sigmask(SIG_BLOCK, &sig_mask, NULL) != 0) {
        perror("pthread_sigmask");
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

    /* Initialize counting semaphore holding available ports */
    if (sem_init(&g_port_sem, 0, (unsigned int)num_ports) < 0) {
        perror("sem_init");
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
        atomic_init(&g_ports[i].fd, -1);
        atomic_init(&g_ports[i].state, PORT_STATE_FREE);
        atomic_init(&g_ports[i].rx_done, false);
        atomic_init(&g_ports[i].tx_done, false);

        for (int p = 0; p < NUM_PRIORITIES; p++) {
            g_ports[i].q_head[p] = 0;
            g_ports[i].q_tail[p] = 0;
            g_ports[i].q_count[p] = 0;
            for (int q = 0; q < PORT_QUEUE_CAPACITY; q++) {
                g_ports[i].queues[p][q].sf = NULL;
            }
        }
        g_ports[i].wrr_current_q = 0;
        g_ports[i].wrr_weight_left = 5;
        g_ports[i].rate_limit = atomic_load_explicit(&g_default_rate_limit, memory_order_relaxed);
        g_ports[i].tokens = g_ports[i].rate_limit;
        g_ports[i].token_ms_accum = 0;

        atomic_init(&g_ports[i].vlan_id, 1);
        atomic_init(&g_ports[i].is_trunk, false);
        atomic_init(&g_ports[i].mirror_dest, -1);

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

    /* Load initial configuration if switch.conf exists */
    reload_config("switch.conf");

    /* Initialize token bucket rate refill timer */
    pthread_mutex_init(&g_rate_timer_mutex, NULL);
    pthread_cond_init(&g_rate_timer_cond, NULL);
    if (pthread_create(&g_rate_timer_tid, NULL, rate_limiter_thread, NULL) != 0) {
        perror("pthread_create rate_limiter_thread");
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

    atomic_store_explicit(&g_listen_fd, listen_fd, memory_order_relaxed);

    /* Spawn dedicated signal handler thread using sigwait */
    pthread_t sig_tid;
    int s_err = pthread_create(&sig_tid, NULL, signal_thread, NULL);
    if (s_err != 0) {
        fprintf(stderr, "pthread_create signal_thread failed: %s\n", strerror(s_err));
        return 1;
    }

    printf("[Switch] Started on port %ld with %ld max ports (QoS WRR 5:3:1 and Token Bucket rate limiter active)\n",
           port, num_ports);
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
            if (!atomic_load_explicit(&g_running, memory_order_relaxed)) {
                break;
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

            /* Wait on counting semaphore for an available port (timeout 2s) */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 2;

            int s_rc = sem_timedwait(&g_port_sem, &ts);
            if (s_rc < 0) {
                if (errno == ETIMEDOUT) {
                    printf("[Switch] Switch full: timed out waiting for free port\n");
                    fflush(stdout);

                    /* Send "switch full" reply frame before closing socket */
                    frame_t full_frame;
                    memset(&full_frame, 0, sizeof(full_frame));
                    full_frame.length = 11;
                    memcpy(full_frame.payload, "switch full", 11);
                    write_frame(client_fd, &full_frame);

                    close(client_fd);
                    continue;
                }
                if (errno == EINTR) {
                    if (!atomic_load_explicit(&g_running, memory_order_relaxed)) {
                        close(client_fd);
                        break;
                    }
                    continue;
                }
                perror("sem_timedwait");
                close(client_fd);
                continue;
            }

            if (!atomic_load_explicit(&g_running, memory_order_relaxed)) {
                close(client_fd);
                sem_post(&g_port_sem);
                break;
            }

            int port_id = allocate_port(client_fd);
            if (port_id < 0) {
                fprintf(stderr, "[Switch] Allocation failed despite semaphore permit\n");
                sem_post(&g_port_sem);
                close(client_fd);
            }
        }
    }

    printf("[Switch] Teardown: waiting for threads and active ports...\n");
    fflush(stdout);

    /* Close listen socket if not already closed */
    int lfd = atomic_exchange_explicit(&g_listen_fd, -1, memory_order_relaxed);
    if (lfd >= 0) {
        shutdown(lfd, SHUT_RDWR);
        close(lfd);
    }

    /* Join signal thread */
    pthread_join(sig_tid, NULL);

    /* Stop and join rate limiter timer thread */
    pthread_mutex_lock(&g_rate_timer_mutex);
    pthread_cond_broadcast(&g_rate_timer_cond);
    pthread_mutex_unlock(&g_rate_timer_mutex);
    pthread_join(g_rate_timer_tid, NULL);
    pthread_mutex_destroy(&g_rate_timer_mutex);
    pthread_cond_destroy(&g_rate_timer_cond);

    /* Unblock all active sender threads and signal closing */
    for (int i = 0; i < g_num_ports; i++) {
        int st = atomic_load_explicit(&g_ports[i].state, memory_order_relaxed);
        if (st == PORT_STATE_ACTIVE || st == PORT_STATE_CLOSING) {
            pthread_mutex_lock(&g_ports[i].queue_mutex);
            atomic_store_explicit(&g_ports[i].state, PORT_STATE_CLOSING, memory_order_relaxed);
            pthread_cond_broadcast(&g_ports[i].queue_cond);
            int fd = atomic_load_explicit(&g_ports[i].fd, memory_order_relaxed);
            pthread_mutex_unlock(&g_ports[i].queue_mutex);
            if (fd >= 0) {
                shutdown(fd, SHUT_RDWR);
            }
        }
    }

    /* Join all port threads and release resources */
    for (int i = 0; i < g_num_ports; i++) {
        int st = atomic_load_explicit(&g_ports[i].state, memory_order_relaxed);
        if (st == PORT_STATE_ACTIVE || st == PORT_STATE_CLOSING || st == PORT_STATE_TERMINATED) {
            int err = pthread_join(g_ports[i].rx_tid, NULL);
            if (err != 0) {
                fprintf(stderr, "pthread_join rx_tid failed: %s\n", strerror(err));
            }
            err = pthread_join(g_ports[i].tx_tid, NULL);
            if (err != 0) {
                fprintf(stderr, "pthread_join tx_tid failed: %s\n", strerror(err));
            }
            int fd = atomic_exchange_explicit(&g_ports[i].fd, -1, memory_order_relaxed);
            if (fd >= 0) {
                if (close(fd) < 0) {
                    perror("close port fd");
                }
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
    sem_destroy(&g_port_sem);
    free(g_ports);

    printf("[Switch] Teardown complete. All resources freed.\n");
    fflush(stdout);
    return 0;
}
