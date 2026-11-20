#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#define MAC_LEN           6
#define MAC_STR_LEN       18
#define FRAME_HEADER_LEN  17
#define MAX_PAYLOAD_LEN   1500
#define MAX_FRAME_LEN     (FRAME_HEADER_LEN + MAX_PAYLOAD_LEN)

#define PRIORITY_HIGH     0
#define PRIORITY_NORMAL   1
#define PRIORITY_LOW      2

extern const uint8_t MAC_BROADCAST[MAC_LEN];

typedef struct {
    uint16_t length;                  /* Payload length in bytes (0 - 1500) */
    uint8_t  src[MAC_LEN];            /* Source MAC address */
    uint8_t  dst[MAC_LEN];            /* Destination MAC address */
    uint16_t vlan;                    /* VLAN number (1 - 4094) */
    uint8_t  priority;                /* Priority level: 0=high, 1=normal, 2=low */
    uint8_t  payload[MAX_PAYLOAD_LEN];/* Frame payload */
} frame_t;

/*
 * Reads exactly count bytes from fd into buf.
 * Handles partial reads and EINTR.
 * Returns count on success, 0 on clean EOF (0 bytes read), or -1 on error/abrupt disconnect.
 */
ssize_t read_full(int fd, void *buf, size_t count);

/*
 * Writes exactly count bytes from buf to fd.
 * Handles partial writes and EINTR.
 * Returns count on success, or -1 on error.
 */
ssize_t write_full(int fd, const void *buf, size_t count);

/*
 * Packs frame into a raw byte buffer (17-byte header followed by payload).
 * Multi-byte fields are packed in network byte order via htons.
 * Returns total serialized byte count on success, or -1 on invalid input.
 */
int frame_pack(const frame_t *frame, uint8_t *buf, size_t buf_len);

/*
 * Unpacks 17-byte wire header from buf into frame structure.
 * Multi-byte fields are converted from network byte order via ntohs.
 * Returns 0 on success, or -1 on invalid header or length > 1500.
 */
int frame_unpack_header(const uint8_t *buf, size_t buf_len, frame_t *frame);

/*
 * Reads a complete frame (17-byte header + payload) from fd.
 * Returns 1 on success, 0 on clean EOF before header, or -1 on error.
 */
int read_frame(int fd, frame_t *frame);

/*
 * Serializes and writes a complete frame to fd.
 * Returns 0 on success, or -1 on error.
 */
int write_frame(int fd, const frame_t *frame);

/*
 * Parses a MAC string in "XX:XX:XX:XX:XX:XX" format.
 * Returns 0 on success, or -1 on invalid format.
 */
int mac_from_str(const char *str, uint8_t mac[MAC_LEN]);

/*
 * Formats a 6-byte MAC into a null-terminated string "XX:XX:XX:XX:XX:XX".
 * out must have space for at least MAC_STR_LEN bytes.
 * Returns 0 on success, or -1 on error.
 */
int mac_to_str(const uint8_t mac[MAC_LEN], char *out, size_t out_len);

/*
 * Returns 1 if mac is FF:FF:FF:FF:FF:FF, 0 otherwise.
 */
int mac_is_broadcast(const uint8_t mac[MAC_LEN]);

/*
 * Returns 1 if mac1 and mac2 are identical, 0 otherwise.
 */
int mac_equals(const uint8_t mac1[MAC_LEN], const uint8_t mac2[MAC_LEN]);

#endif /* PROTOCOL_H */
