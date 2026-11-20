#include "protocol.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <arpa/inet.h>

const uint8_t MAC_BROADCAST[MAC_LEN] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};

ssize_t read_full(int fd, void *buf, size_t count) {
    size_t total_read = 0;
    uint8_t *ptr = (uint8_t *)buf;

    while (total_read < count) {
        ssize_t n = read(fd, ptr + total_read, count - total_read);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            if (total_read == 0) {
                return 0; /* Clean disconnect between frames */
            }
            /* Abrupt disconnect in the middle of a frame */
            errno = ECONNRESET;
            return -1;
        }
        total_read += (size_t)n;
    }
    return (ssize_t)total_read;
}

ssize_t write_full(int fd, const void *buf, size_t count) {
    size_t total_written = 0;
    const uint8_t *ptr = (const uint8_t *)buf;

    while (total_written < count) {
        ssize_t n = write(fd, ptr + total_written, count - total_written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            errno = EIO;
            return -1;
        }
        total_written += (size_t)n;
    }
    return (ssize_t)total_written;
}

int frame_pack(const frame_t *frame, uint8_t *buf, size_t buf_len) {
    if (!frame || !buf || frame->length > MAX_PAYLOAD_LEN) {
        return -1;
    }
    size_t total_len = FRAME_HEADER_LEN + frame->length;
    if (buf_len < total_len) {
        return -1;
    }

    /* Explicit packing avoids compiler padding/endianness bugs */
    uint16_t net_len = htons(frame->length);
    memcpy(buf + 0, &net_len, sizeof(net_len));
    memcpy(buf + 2, frame->src, MAC_LEN);
    memcpy(buf + 8, frame->dst, MAC_LEN);
    uint16_t net_vlan = htons(frame->vlan);
    memcpy(buf + 14, &net_vlan, sizeof(net_vlan));
    buf[16] = frame->priority;

    if (frame->length > 0) {
        memcpy(buf + FRAME_HEADER_LEN, frame->payload, frame->length);
    }
    return (int)total_len;
}

int frame_unpack_header(const uint8_t *buf, size_t buf_len, frame_t *frame) {
    if (!buf || !frame || buf_len < FRAME_HEADER_LEN) {
        return -1;
    }

    uint16_t net_len;
    memcpy(&net_len, buf + 0, sizeof(net_len));
    frame->length = ntohs(net_len);
    if (frame->length > MAX_PAYLOAD_LEN) {
        return -1;
    }

    memcpy(frame->src, buf + 2, MAC_LEN);
    memcpy(frame->dst, buf + 8, MAC_LEN);

    uint16_t net_vlan;
    memcpy(&net_vlan, buf + 14, sizeof(net_vlan));
    frame->vlan = ntohs(net_vlan);

    frame->priority = buf[16];
    return 0;
}

int read_frame(int fd, frame_t *frame) {
    if (fd < 0 || !frame) {
        errno = EINVAL;
        return -1;
    }

    uint8_t header[FRAME_HEADER_LEN];
    ssize_t n = read_full(fd, header, FRAME_HEADER_LEN);
    if (n == 0) {
        return 0; /* Clean EOF */
    }
    if (n < 0) {
        return -1;
    }

    if (frame_unpack_header(header, FRAME_HEADER_LEN, frame) < 0) {
        errno = EPROTO;
        return -1;
    }

    if (frame->length > 0) {
        ssize_t p_bytes = read_full(fd, frame->payload, frame->length);
        if (p_bytes <= 0) {
            if (p_bytes == 0) {
                errno = ECONNRESET;
            }
            return -1;
        }
    }

    return 1;
}

int write_frame(int fd, const frame_t *frame) {
    if (fd < 0 || !frame || frame->length > MAX_PAYLOAD_LEN) {
        errno = EINVAL;
        return -1;
    }

    uint8_t buf[MAX_FRAME_LEN];
    int total_len = frame_pack(frame, buf, sizeof(buf));
    if (total_len < 0) {
        errno = EINVAL;
        return -1;
    }

    if (write_full(fd, buf, (size_t)total_len) < 0) {
        return -1;
    }
    return 0;
}

int mac_from_str(const char *str, uint8_t mac[MAC_LEN]) {
    if (!str || !mac) {
        return -1;
    }
    if (strlen(str) != 17) {
        return -1;
    }
    for (int i = 0; i < 17; i++) {
        if (i % 3 == 2) {
            if (str[i] != ':') {
                return -1;
            }
        } else {
            if (!isxdigit((unsigned char)str[i])) {
                return -1;
            }
        }
    }

    unsigned int bytes[MAC_LEN];
    if (sscanf(str, "%2x:%2x:%2x:%2x:%2x:%2x",
               &bytes[0], &bytes[1], &bytes[2],
               &bytes[3], &bytes[4], &bytes[5]) != 6) {
        return -1;
    }
    for (int i = 0; i < MAC_LEN; i++) {
        mac[i] = (uint8_t)bytes[i];
    }
    return 0;
}

int mac_to_str(const uint8_t mac[MAC_LEN], char *out, size_t out_len) {
    if (!mac || !out || out_len < MAC_STR_LEN) {
        return -1;
    }
    int n = snprintf(out, out_len, "%02X:%02X:%02X:%02X:%02X:%02X",
                     mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
    if (n < 0 || (size_t)n >= out_len) {
        return -1;
    }
    return 0;
}

int mac_is_broadcast(const uint8_t mac[MAC_LEN]) {
    if (!mac) {
        return 0;
    }
    return memcmp(mac, MAC_BROADCAST, MAC_LEN) == 0;
}

int mac_equals(const uint8_t mac1[MAC_LEN], const uint8_t mac2[MAC_LEN]) {
    if (!mac1 || !mac2) {
        return 0;
    }
    return memcmp(mac1, mac2, MAC_LEN) == 0;
}
