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
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static _Atomic int g_host_running = 1;

typedef struct {
    int sock_fd;
    char host_mac_str[MAC_STR_LEN];
} host_rx_args_t;

static void *host_receiver(void *arg) {
    host_rx_args_t *args = (host_rx_args_t *)arg;
    int fd = args->sock_fd;

    while (atomic_load_explicit(&g_host_running, memory_order_relaxed)) {
        frame_t rx_frame;
        int rc = read_frame(fd, &rx_frame);
        if (rc == 0) {
            if (atomic_load_explicit(&g_host_running, memory_order_relaxed)) {
                printf("[Host %s] Switch closed connection\n", args->host_mac_str);
                fflush(stdout);
            }
            break;
        }
        if (rc < 0) {
            if (atomic_load_explicit(&g_host_running, memory_order_relaxed) && errno != EINTR && errno != EBADF) {
                printf("[Host %s] Receive error: %s\n", args->host_mac_str, strerror(errno));
                fflush(stdout);
            }
            break;
        }

        char src_str[MAC_STR_LEN];
        char dst_str[MAC_STR_LEN];
        if (mac_to_str(rx_frame.src, src_str, sizeof(src_str)) < 0) {
            strncpy(src_str, "??:??:??:??:??:??", sizeof(src_str));
        }
        if (mac_to_str(rx_frame.dst, dst_str, sizeof(dst_str)) < 0) {
            strncpy(dst_str, "??:??:??:??:??:??", sizeof(dst_str));
        }

        printf("[Host %s] RX: src=%s dst=%s vlan=%u prio=%u len=%u payload='%.*s'\n",
               args->host_mac_str, src_str, dst_str, rx_frame.vlan, rx_frame.priority,
               rx_frame.length, (int)rx_frame.length, rx_frame.payload);
        fflush(stdout);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <switch_ip> <tcp_port> <mac> [dst_mac] [payload]\n", argv[0]);
        return 1;
    }

    const char *switch_ip = argv[1];
    char *endptr = NULL;
    long port = strtol(argv[2], &endptr, 10);
    if (*endptr != '\0' || port <= 0 || port > 65535) {
        fprintf(stderr, "Error: Invalid tcp_port: %s\n", argv[2]);
        return 1;
    }

    uint8_t host_mac[MAC_LEN];
    if (mac_from_str(argv[3], host_mac) < 0) {
        fprintf(stderr, "Error: Invalid MAC address: %s (expected XX:XX:XX:XX:XX:XX)\n", argv[3]);
        return 1;
    }

    char host_mac_str[MAC_STR_LEN];
    if (mac_to_str(host_mac, host_mac_str, sizeof(host_mac_str)) < 0) {
        fprintf(stderr, "Error formatting host MAC\n");
        return 1;
    }

    /* Ignore SIGPIPE */
    struct sigaction sa_pipe;
    memset(&sa_pipe, 0, sizeof(sa_pipe));
    sa_pipe.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa_pipe, NULL) < 0) {
        perror("sigaction(SIGPIPE)");
        return 1;
    }

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, switch_ip, &serv_addr.sin_addr) <= 0) {
        fprintf(stderr, "Error: Invalid switch IP address: %s\n", switch_ip);
        close(sock_fd);
        return 1;
    }

    if (connect(sock_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        close(sock_fd);
        return 1;
    }

    printf("[Host %s] Connected to switch at %s:%ld\n", host_mac_str, switch_ip, port);
    fflush(stdout);

    /* If destination MAC is specified on CLI, send one frame and wait for response */
    if (argc >= 5) {
        uint8_t dst_mac[MAC_LEN];
        if (mac_from_str(argv[4], dst_mac) < 0) {
            fprintf(stderr, "Error: Invalid dst_mac: %s\n", argv[4]);
            close(sock_fd);
            return 1;
        }

        const char *payload = (argc >= 6) ? argv[5] : "";
        size_t p_len = strlen(payload);
        if (p_len > MAX_PAYLOAD_LEN) {
            p_len = MAX_PAYLOAD_LEN;
        }

        frame_t tx_frame;
        memset(&tx_frame, 0, sizeof(tx_frame));
        tx_frame.length = (uint16_t)p_len;
        memcpy(tx_frame.src, host_mac, MAC_LEN);
        memcpy(tx_frame.dst, dst_mac, MAC_LEN);
        tx_frame.vlan = 1;
        tx_frame.priority = PRIORITY_NORMAL;
        memcpy(tx_frame.payload, payload, p_len);

        if (write_frame(sock_fd, &tx_frame) < 0) {
            perror("write_frame");
            close(sock_fd);
            return 1;
        }
        printf("[Host %s] TX: dst=%s len=%u payload='%.*s'\n",
               host_mac_str, argv[4], (unsigned int)p_len, (int)p_len, payload);
        fflush(stdout);

        /* Set read timeout so client does not hang if switch is unresponsive */
        struct timeval tv;
        tv.tv_sec = 3;
        tv.tv_usec = 0;
        if (setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
            perror("setsockopt(SO_RCVTIMEO)");
        }

        frame_t rx_frame;
        int rc = read_frame(sock_fd, &rx_frame);
        if (rc == 1) {
            char rx_src[MAC_STR_LEN];
            char rx_dst[MAC_STR_LEN];
            mac_to_str(rx_frame.src, rx_src, sizeof(rx_src));
            mac_to_str(rx_frame.dst, rx_dst, sizeof(rx_dst));
            printf("[Host %s] RX: src=%s dst=%s vlan=%u prio=%u len=%u payload='%.*s'\n",
                   host_mac_str, rx_src, rx_dst, rx_frame.vlan, rx_frame.priority,
                   rx_frame.length, (int)rx_frame.length, rx_frame.payload);
            fflush(stdout);
        } else if (rc == 0) {
            printf("[Host %s] Switch closed connection\n", host_mac_str);
        } else {
            perror("read_frame");
            close(sock_fd);
            return 1;
        }

        if (close(sock_fd) < 0) {
            perror("close sock_fd");
            return 1;
        }
        return 0;
    }

    /* Interactive streaming mode */
    host_rx_args_t rx_args;
    rx_args.sock_fd = sock_fd;
    strncpy(rx_args.host_mac_str, host_mac_str, sizeof(rx_args.host_mac_str) - 1);
    rx_args.host_mac_str[sizeof(rx_args.host_mac_str) - 1] = '\0';

    pthread_t rx_tid;
    int err = pthread_create(&rx_tid, NULL, host_receiver, &rx_args);
    if (err != 0) {
        fprintf(stderr, "pthread_create failed: %s\n", strerror(err));
        close(sock_fd);
        return 1;
    }

    char line[2048];
    while (fgets(line, sizeof(line), stdin)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        char dst_str[64];
        char payload_str[1501];
        payload_str[0] = '\0';

        if (sscanf(line, "%63s %1500[^\n]", dst_str, payload_str) < 1) {
            continue;
        }

        uint8_t dst_mac[MAC_LEN];
        if (mac_from_str(dst_str, dst_mac) < 0) {
            fprintf(stderr, "Error: Invalid destination MAC: %s\n", dst_str);
            continue;
        }

        frame_t tx_frame;
        memset(&tx_frame, 0, sizeof(tx_frame));
        tx_frame.length = (uint16_t)strlen(payload_str);
        memcpy(tx_frame.src, host_mac, MAC_LEN);
        memcpy(tx_frame.dst, dst_mac, MAC_LEN);
        tx_frame.vlan = 1;
        tx_frame.priority = PRIORITY_NORMAL;
        memcpy(tx_frame.payload, payload_str, tx_frame.length);

        if (write_frame(sock_fd, &tx_frame) < 0) {
            perror("write_frame");
            break;
        }
    }

    /* Signal switch that we have finished sending frames */
    if (shutdown(sock_fd, SHUT_WR) < 0 && errno != ENOTCONN) {
        perror("shutdown(SHUT_WR)");
    }
    err = pthread_join(rx_tid, NULL);
    if (err != 0) {
        fprintf(stderr, "pthread_join failed: %s\n", strerror(err));
    }

    if (close(sock_fd) < 0) {
        perror("close sock_fd");
    }

    return 0;
}
