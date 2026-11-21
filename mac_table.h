#ifndef MAC_TABLE_H
#define MAC_TABLE_H

#include "protocol.h"
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>

#define MAC_TABLE_BUCKETS 64
#define MAX_SWITCH_PORTS  128

#define MAC_TABLE_OK                  0
#define MAC_TABLE_MOVED               1
#define MAC_TABLE_SECURITY_VIOLATION -2
#define MAC_TABLE_ERR                -1

typedef struct mac_entry {
    uint8_t mac[MAC_LEN];
    int port_id;
    time_t last_seen;
    struct mac_entry *next;
} mac_entry_t;

typedef struct {
    pthread_mutex_t lock;
    mac_entry_t *head;
} mac_bucket_t;

typedef struct {
    mac_bucket_t buckets[MAC_TABLE_BUCKETS];
    int max_entries;                       /* Global capacity K */
    int max_per_port;                      /* Port security limit M */
    int aging_seconds;                     /* Aging timeout T */
    _Atomic int total_entries;
    _Atomic int port_mac_count[MAX_SWITCH_PORTS];

    _Atomic bool running;
    pthread_t aging_tid;
    pthread_mutex_t aging_mutex;
    pthread_cond_t aging_cond;
} mac_table_t;

/*
 * Initializes the MAC table with striped bucket locks, LRU capacity, port security,
 * and launches background aging thread.
 * Returns 0 on success, -1 on error.
 */
int mac_table_init(mac_table_t *table, int max_entries_k, int max_per_port_m, int aging_time_t);

/*
 * Learns or refreshes source MAC mapping on port_id.
 * Checks port security limit M and global capacity K with LRU eviction.
 * Detects and logs MAC moves.
 * Returns MAC_TABLE_OK, MAC_TABLE_MOVED, or MAC_TABLE_SECURITY_VIOLATION.
 */
int mac_table_learn(mac_table_t *table, const uint8_t mac[MAC_LEN], int port_id);

/*
 * Looks up destination port for given MAC address.
 * Updates last_seen timestamp under bucket lock.
 * Returns port_id if found, or -1 if unknown or broadcast.
 */
int mac_table_lookup(mac_table_t *table, const uint8_t mac[MAC_LEN]);

/*
 * Removes all MAC entries associated with a port (e.g. when port disconnects).
 */
void mac_table_remove_port(mac_table_t *table, int port_id);

/*
 * Cleans up all entries, stops aging thread, and frees resources.
 */
void mac_table_destroy(mac_table_t *table);

#endif /* MAC_TABLE_H */
