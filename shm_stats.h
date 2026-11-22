#ifndef SHM_STATS_H
#define SHM_STATS_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#define SHM_STATS_NAME "/vswitch_stats"
#define UNIX_SOCKET_PATH "/tmp/switch.sock"
#define MAX_SWITCH_PORTS 128

typedef struct {
    int port_id;
    bool is_active;
    bool is_admin_down;
    uint16_t vlan;
    bool is_trunk;
    int mirror_dest;
    uint64_t frames_in;
    uint64_t frames_out;
    uint64_t frames_dropped;
    uint64_t frames_mirrored;
} port_stats_t;

typedef struct {
    pthread_mutex_t lock; /* PTHREAD_PROCESS_SHARED */
    int num_ports;
    port_stats_t ports[MAX_SWITCH_PORTS];
} switch_shm_t;

#endif /* SHM_STATS_H */
