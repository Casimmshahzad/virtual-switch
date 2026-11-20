#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static _Atomic int g_running = 1;

typedef struct {
    int fd;
    int port_id;
} client_context_t;

static void handle_signal(int sig) {
    (void)sig;
    int saved_errno = errno;
    atomic_store_explicit(&g_running, 0, memory_order_relaxed);
    errno = saved_errno;
}

static void *client_worker(void *arg) {
    client_context_t *ctx = (client_context_t *)arg;
    int fd = ctx->fd;
    int port_id = ctx->port_id;
    free(ctx);

    printf("[Switch] Port %d: Host connected\n", port_id);
    fflush(stdout);

    frame_t frame;
    while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
        int rc = read_frame(fd, &frame);
        if (rc == 0) {
            printf("[Switch] Port %d: Host disconnected cleanly\n", port_id);
            fflush(stdout);
            break;
        }
        if (rc < 0) {
            if (atomic_load_explicit(&g_running, memory_order_relaxed)) {
                printf("[Switch] Port %d: Read error / abrupt disconnect: %s\n",
                       port_id, strerror(errno));
                fflush(stdout);
            }
            break;
        }

        char src_str[MAC_STR_LEN];
        char dst_str[MAC_STR_LEN];
        if (mac_to_str(frame.src, src_str, sizeof(src_str)) < 0) {
            strncpy(src_str, "??:??:??:??:??:??", sizeof(src_str));
        }
        if (mac_to_str(frame.dst, dst_str, sizeof(dst_str)) < 0) {
            strncpy(dst_str, "??:??:??:??:??:??", sizeof(dst_str));
        }

        printf("[Switch] Port %d: Frame received src=%s dst=%s vlan=%u prio=%u len=%u\n",
               port_id, src_str, dst_str, frame.vlan, frame.priority, frame.length);
        fflush(stdout);

        /* Echo frame back for Milestone 1 wire protocol verification */
        if (write_frame(fd, &frame) < 0) {
            printf("[Switch] Port %d: Write failed: %s\n", port_id, strerror(errno));
            fflush(stdout);
            break;
        }
    }

    if (close(fd) < 0) {
        perror("close client fd");
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
    if (*endptr != '\0' || num_ports <= 0) {
        fprintf(stderr, "Error: Invalid num_ports: %s\n", argv[2]);
        return 1;
    }

    /* Ignore SIGPIPE so write() returns EPIPE rather than killing the switch */
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

    int next_port_id = 0;
    while (atomic_load_explicit(&g_running, memory_order_relaxed)) {
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

        client_context_t *ctx = malloc(sizeof(*ctx));
        if (!ctx) {
            perror("malloc client_context");
            close(client_fd);
            continue;
        }
        ctx->fd = client_fd;
        ctx->port_id = next_port_id++;

        pthread_t tid;
        int err = pthread_create(&tid, NULL, client_worker, ctx);
        if (err != 0) {
            fprintf(stderr, "pthread_create failed: %s\n", strerror(err));
            close(client_fd);
            free(ctx);
            continue;
        }

        err = pthread_detach(tid);
        if (err != 0) {
            fprintf(stderr, "pthread_detach failed: %s\n", strerror(err));
        }
    }

    printf("[Switch] Shutting down...\n");
    if (close(listen_fd) < 0) {
        perror("close listen_fd");
    }

    return 0;
}
