#include "mac_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

static inline unsigned int mac_hash(const uint8_t mac[MAC_LEN]) {
    unsigned int hash = 5381;
    for (int i = 0; i < MAC_LEN; i++) {
        hash = ((hash << 5) + hash) + mac[i];
    }
    return hash % MAC_TABLE_BUCKETS;
}

/*
 * Evicts least-recently-used entry across all buckets.
 * Operates one bucket lock at a time to prevent deadlocks.
 */
static void evict_lru_entry(mac_table_t *table) {
    int best_bucket = -1;
    time_t oldest_time = (time_t)0x7FFFFFFF;
    uint8_t oldest_mac[MAC_LEN];
    bool found = false;

    for (int b = 0; b < MAC_TABLE_BUCKETS; b++) {
        pthread_mutex_lock(&table->buckets[b].lock);
        mac_entry_t *curr = table->buckets[b].head;
        while (curr) {
            if (curr->last_seen < oldest_time) {
                oldest_time = curr->last_seen;
                memcpy(oldest_mac, curr->mac, MAC_LEN);
                best_bucket = b;
                found = true;
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&table->buckets[b].lock);
    }

    if (!found || best_bucket < 0) {
        return;
    }

    pthread_mutex_lock(&table->buckets[best_bucket].lock);
    mac_entry_t **pcurr = &table->buckets[best_bucket].head;
    while (*pcurr) {
        mac_entry_t *target = *pcurr;
        if (mac_equals(target->mac, oldest_mac)) {
            *pcurr = target->next;
            atomic_fetch_sub_explicit(&table->port_mac_count[target->port_id], 1, memory_order_relaxed);
            atomic_fetch_sub_explicit(&table->total_entries, 1, memory_order_relaxed);

            char mac_str[MAC_STR_LEN];
            mac_to_str(target->mac, mac_str, sizeof(mac_str));
            printf("[MAC Table] LRU eviction: removed MAC %s from port %d\n",
                   mac_str, target->port_id);
            fflush(stdout);

            free(target);
            break;
        }
        pcurr = &target->next;
    }
    pthread_mutex_unlock(&table->buckets[best_bucket].lock);
}

/*
 * Background aging thread: removes entries older than aging_seconds.
 */
static void *mac_table_aging_thread(void *arg) {
    mac_table_t *table = (mac_table_t *)arg;

    while (atomic_load_explicit(&table->running, memory_order_relaxed)) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;

        pthread_mutex_lock(&table->aging_mutex);
        while (atomic_load_explicit(&table->running, memory_order_relaxed)) {
            int rc = pthread_cond_timedwait(&table->aging_cond, &table->aging_mutex, &ts);
            if (rc == ETIMEDOUT) {
                break;
            }
        }
        pthread_mutex_unlock(&table->aging_mutex);

        if (!atomic_load_explicit(&table->running, memory_order_relaxed)) {
            break;
        }

        time_t now = time(NULL);
        for (int b = 0; b < MAC_TABLE_BUCKETS; b++) {
            pthread_mutex_lock(&table->buckets[b].lock);
            mac_entry_t **pcurr = &table->buckets[b].head;
            while (*pcurr) {
                mac_entry_t *entry = *pcurr;
                if (now - entry->last_seen >= atomic_load_explicit(&table->aging_seconds, memory_order_relaxed)) {
                    *pcurr = entry->next;
                    atomic_fetch_sub_explicit(&table->port_mac_count[entry->port_id], 1, memory_order_relaxed);
                    atomic_fetch_sub_explicit(&table->total_entries, 1, memory_order_relaxed);

                    char mac_str[MAC_STR_LEN];
                    mac_to_str(entry->mac, mac_str, sizeof(mac_str));
                    printf("[MAC Table] Aging: removed MAC %s from port %d (idle %ld s)\n",
                           mac_str, entry->port_id, (long)(now - entry->last_seen));
                    fflush(stdout);

                    free(entry);
                } else {
                    pcurr = &entry->next;
                }
            }
            pthread_mutex_unlock(&table->buckets[b].lock);
        }
    }
    return NULL;
}

int mac_table_init(mac_table_t *table, int max_entries_k, int max_per_port_m, int aging_time_t) {
    if (!table) {
        return -1;
    }

    table->max_entries = (max_entries_k > 0) ? max_entries_k : 1024;
    atomic_init(&table->max_per_port, (max_per_port_m > 0) ? max_per_port_m : 16);
    atomic_init(&table->aging_seconds, (aging_time_t > 0) ? aging_time_t : 30);

    atomic_init(&table->total_entries, 0);
    for (int i = 0; i < MAX_SWITCH_PORTS; i++) {
        atomic_init(&table->port_mac_count[i], 0);
    }
    atomic_init(&table->running, true);

    for (int b = 0; b < MAC_TABLE_BUCKETS; b++) {
        int err = pthread_mutex_init(&table->buckets[b].lock, NULL);
        if (err != 0) {
            fprintf(stderr, "pthread_mutex_init bucket failed: %s\n", strerror(err));
            return -1;
        }
        table->buckets[b].head = NULL;
    }

    int err = pthread_mutex_init(&table->aging_mutex, NULL);
    if (err != 0) {
        fprintf(stderr, "pthread_mutex_init aging failed: %s\n", strerror(err));
        return -1;
    }
    err = pthread_cond_init(&table->aging_cond, NULL);
    if (err != 0) {
        fprintf(stderr, "pthread_cond_init aging failed: %s\n", strerror(err));
        return -1;
    }

    err = pthread_create(&table->aging_tid, NULL, mac_table_aging_thread, table);
    if (err != 0) {
        fprintf(stderr, "pthread_create aging thread failed: %s\n", strerror(err));
        return -1;
    }

    return 0;
}

int mac_table_learn(mac_table_t *table, const uint8_t mac[MAC_LEN], int port_id) {
    if (!table || !mac || port_id < 0 || port_id >= MAX_SWITCH_PORTS) {
        return MAC_TABLE_ERR;
    }
    if (mac_is_broadcast(mac)) {
        return MAC_TABLE_OK;
    }

    unsigned int b = mac_hash(mac);
    time_t now = time(NULL);

    pthread_mutex_lock(&table->buckets[b].lock);
    mac_entry_t *entry = table->buckets[b].head;
    while (entry) {
        if (mac_equals(entry->mac, mac)) {
            entry->last_seen = now;
            if (entry->port_id != port_id) {
                /* Host moved to a new port */
                int old_port = entry->port_id;
                if (atomic_load_explicit(&table->port_mac_count[port_id], memory_order_relaxed) >= table->max_per_port) {
                    pthread_mutex_unlock(&table->buckets[b].lock);
                    printf("[Port Security] Port %d exceeded limit of %d MACs on move: shutting down port!\n",
                           port_id, table->max_per_port);
                    fflush(stdout);
                    return MAC_TABLE_SECURITY_VIOLATION;
                }

                atomic_fetch_sub_explicit(&table->port_mac_count[old_port], 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&table->port_mac_count[port_id], 1, memory_order_relaxed);
                entry->port_id = port_id;

                char mac_str[MAC_STR_LEN];
                mac_to_str(mac, mac_str, sizeof(mac_str));
                printf("[MAC Table] MAC %s moved from port %d to port %d (MAC moved)\n",
                       mac_str, old_port, port_id);
                fflush(stdout);
                pthread_mutex_unlock(&table->buckets[b].lock);
                return MAC_TABLE_MOVED;
            }

            pthread_mutex_unlock(&table->buckets[b].lock);
            return MAC_TABLE_OK;
        }
        entry = entry->next;
    }

    /* New MAC: enforce port security limit M */
    int limit = atomic_load_explicit(&table->max_per_port, memory_order_relaxed);
    if (atomic_load_explicit(&table->port_mac_count[port_id], memory_order_relaxed) >= limit) {
        pthread_mutex_unlock(&table->buckets[b].lock);
        printf("[Port Security] Port %d exceeded limit of %d MACs: shutting down port!\n",
               port_id, limit);
        fflush(stdout);
        return MAC_TABLE_SECURITY_VIOLATION;
    }

    /* Enforce global capacity cap K with LRU eviction */
    if (atomic_load_explicit(&table->total_entries, memory_order_relaxed) >= table->max_entries) {
        pthread_mutex_unlock(&table->buckets[b].lock);
        evict_lru_entry(table);
        pthread_mutex_lock(&table->buckets[b].lock);

        /* Recheck if another thread inserted this MAC while unlocked */
        entry = table->buckets[b].head;
        while (entry) {
            if (mac_equals(entry->mac, mac)) {
                entry->last_seen = now;
                pthread_mutex_unlock(&table->buckets[b].lock);
                return MAC_TABLE_OK;
            }
            entry = entry->next;
        }
    }

    mac_entry_t *new_entry = malloc(sizeof(*new_entry));
    if (!new_entry) {
        pthread_mutex_unlock(&table->buckets[b].lock);
        return MAC_TABLE_ERR;
    }
    memcpy(new_entry->mac, mac, MAC_LEN);
    new_entry->port_id = port_id;
    new_entry->last_seen = now;
    new_entry->next = table->buckets[b].head;
    table->buckets[b].head = new_entry;

    atomic_fetch_add_explicit(&table->port_mac_count[port_id], 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&table->total_entries, 1, memory_order_relaxed);

    char mac_str[MAC_STR_LEN];
    mac_to_str(mac, mac_str, sizeof(mac_str));
    printf("[MAC Table] Learned MAC %s on port %d (total: %d)\n",
           mac_str, port_id, atomic_load_explicit(&table->total_entries, memory_order_relaxed));
    fflush(stdout);

    pthread_mutex_unlock(&table->buckets[b].lock);
    return MAC_TABLE_OK;
}

int mac_table_lookup(mac_table_t *table, const uint8_t mac[MAC_LEN]) {
    if (!table || !mac) {
        return -1;
    }
    if (mac_is_broadcast(mac)) {
        return -1;
    }

    unsigned int b = mac_hash(mac);
    time_t now = time(NULL);
    int port_id = -1;

    pthread_mutex_lock(&table->buckets[b].lock);
    mac_entry_t *entry = table->buckets[b].head;
    while (entry) {
        if (mac_equals(entry->mac, mac)) {
            entry->last_seen = now;
            port_id = entry->port_id;
            break;
        }
        entry = entry->next;
    }
    pthread_mutex_unlock(&table->buckets[b].lock);

    return port_id;
}

void mac_table_remove_port(mac_table_t *table, int port_id) {
    if (!table || port_id < 0 || port_id >= MAX_SWITCH_PORTS) {
        return;
    }

    for (int b = 0; b < MAC_TABLE_BUCKETS; b++) {
        pthread_mutex_lock(&table->buckets[b].lock);
        mac_entry_t **pcurr = &table->buckets[b].head;
        while (*pcurr) {
            mac_entry_t *entry = *pcurr;
            if (entry->port_id == port_id) {
                *pcurr = entry->next;
                atomic_fetch_sub_explicit(&table->total_entries, 1, memory_order_relaxed);
                free(entry);
            } else {
                pcurr = &entry->next;
            }
        }
        pthread_mutex_unlock(&table->buckets[b].lock);
    }
    atomic_store_explicit(&table->port_mac_count[port_id], 0, memory_order_relaxed);
}

int mac_table_dump(mac_table_t *table, const char *filepath) {
    if (!table || !filepath) {
        return -1;
    }

    FILE *f = fopen(filepath, "w");
    if (!f) {
        perror("fopen mac_dump.txt");
        return -1;
    }

    time_t now = time(NULL);
    fprintf(f, "# MAC Table Dump\n");
    fprintf(f, "# Total entries: %d\n", atomic_load_explicit(&table->total_entries, memory_order_relaxed));
    fprintf(f, "# %-17s  %-5s  %-8s\n", "MAC Address", "Port", "Idle(s)");

    for (int b = 0; b < MAC_TABLE_BUCKETS; b++) {
        pthread_mutex_lock(&table->buckets[b].lock);
        mac_entry_t *entry = table->buckets[b].head;
        while (entry) {
            char mac_str[MAC_STR_LEN];
            mac_to_str(entry->mac, mac_str, sizeof(mac_str));
            long idle = (long)(now - entry->last_seen);
            fprintf(f, "%-17s  %-5d  %ld\n", mac_str, entry->port_id, idle);
            entry = entry->next;
        }
        pthread_mutex_unlock(&table->buckets[b].lock);
    }

    fclose(f);
    return 0;
}

void mac_table_destroy(mac_table_t *table) {
    if (!table) {
        return;
    }

    /* Stop aging thread */
    atomic_store_explicit(&table->running, false, memory_order_relaxed);
    pthread_mutex_lock(&table->aging_mutex);
    pthread_cond_broadcast(&table->aging_cond);
    pthread_mutex_unlock(&table->aging_mutex);

    pthread_join(table->aging_tid, NULL);
    pthread_mutex_destroy(&table->aging_mutex);
    pthread_cond_destroy(&table->aging_cond);

    /* Free all bucket entries and destroy mutexes */
    for (int b = 0; b < MAC_TABLE_BUCKETS; b++) {
        pthread_mutex_lock(&table->buckets[b].lock);
        mac_entry_t *curr = table->buckets[b].head;
        while (curr) {
            mac_entry_t *next = curr->next;
            free(curr);
            curr = next;
        }
        table->buckets[b].head = NULL;
        pthread_mutex_unlock(&table->buckets[b].lock);
        pthread_mutex_destroy(&table->buckets[b].lock);
    }
}
