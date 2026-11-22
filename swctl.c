#include "shm_stats.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>

static volatile sig_atomic_t g_stop = 0;
static void handle_sig(int sig) {
    (void)sig;
    g_stop = 1;
}

static void print_stats_table(switch_shm_t *shm) {
    pthread_mutex_lock(&shm->lock);
    printf("%-5s  %-5s  %-6s  %-6s  %-10s  %-10s  %-10s  %-10s  %-10s  %-6s\n",
           "PORT", "VLAN", "TRUNK", "STATUS", "STATE", "RX_FRAMES", "TX_FRAMES", "DROPPED", "MIRRORED", "MIRROR");
    for (int i = 0; i < shm->num_ports; i++) {
        port_stats_t *p = &shm->ports[i];
        const char *status = p->is_admin_down ? "DOWN" : "UP";
        const char *state = p->is_active ? "ACTIVE" : "FREE";
        const char *trunk = p->is_trunk ? "yes" : "no";
        char mirror_str[16];
        if (p->mirror_dest >= 0) {
            snprintf(mirror_str, sizeof(mirror_str), "-> %d", p->mirror_dest);
        } else {
            strncpy(mirror_str, "none", sizeof(mirror_str));
        }

        printf("%-5d  %-5u  %-6s  %-6s  %-10s  %-10lu  %-10lu  %-10lu  %-10lu  %-6s\n",
               p->port_id, p->vlan, trunk, status, state,
               (unsigned long)p->frames_in, (unsigned long)p->frames_out,
               (unsigned long)p->frames_dropped, (unsigned long)p->frames_mirrored,
               mirror_str);
    }
    pthread_mutex_unlock(&shm->lock);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <command> [args...]\n", argv[0]);
        fprintf(stderr, "Commands:\n");
        fprintf(stderr, "  show mac\n");
        fprintf(stderr, "  show ports\n");
        fprintf(stderr, "  shutdown <port>\n");
        fprintf(stderr, "  up <port>\n");
        fprintf(stderr, "  vlan <port> <vlan>\n");
        fprintf(stderr, "  mirror <port> <sniffer_port>\n");
        fprintf(stderr, "  clear mac\n");
        fprintf(stderr, "  stats\n");
        fprintf(stderr, "  stats --watch\n");
        return 1;
    }

    if (strcmp(argv[1], "stats") == 0) {
        bool watch = (argc >= 3 && strcmp(argv[2], "--watch") == 0);
        int shm_fd = shm_open(SHM_STATS_NAME, O_RDWR, 0666);
        if (shm_fd < 0) {
            perror("shm_open (is switch running?)");
            return 1;
        }
        switch_shm_t *shm = mmap(NULL, sizeof(switch_shm_t), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        if (shm == MAP_FAILED) {
            perror("mmap");
            close(shm_fd);
            return 1;
        }
        close(shm_fd);

        if (!watch) {
            print_stats_table(shm);
        } else {
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sa.sa_handler = handle_sig;
            sigaction(SIGINT, &sa, NULL);
            sigaction(SIGTERM, &sa, NULL);

            while (!g_stop) {
                printf("\033[H\033[J"); /* Clear screen */
                printf("=== Switch Live Shared Memory Stats (Press Ctrl+C to exit) ===\n\n");
                print_stats_table(shm);
                sleep(1);
            }
        }
        munmap(shm, sizeof(switch_shm_t));
        return 0;
    }

    /* All other commands: send over UNIX domain socket */
    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, UNIX_SOCKET_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect to /tmp/switch.sock (is switch running?)");
        close(sock_fd);
        return 1;
    }

    char cmd[512] = {0};
    for (int i = 1; i < argc; i++) {
        if (i > 1) strncat(cmd, " ", sizeof(cmd) - strlen(cmd) - 1);
        strncat(cmd, argv[i], sizeof(cmd) - strlen(cmd) - 1);
    }
    strncat(cmd, "\n", sizeof(cmd) - strlen(cmd) - 1);

    if (write(sock_fd, cmd, strlen(cmd)) < 0) {
        perror("write command");
        close(sock_fd);
        return 1;
    }

    char reply[4096];
    ssize_t n;
    while ((n = read(sock_fd, reply, sizeof(reply) - 1)) > 0) {
        reply[n] = '\0';
        fputs(reply, stdout);
    }
    fflush(stdout);

    close(sock_fd);
    return 0;
}
