CC ?= gcc
CFLAGS ?= -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -g -O0 -pthread
LDFLAGS ?= -pthread -lrt

SWITCH_OBJS = protocol.o mac_table.o
HOST_OBJS = protocol.o
TARGETS = switch host switch_tsan host_tsan swctl

all: switch host swctl

%.o: %.c protocol.h mac_table.h shm_stats.h
	$(CC) $(CFLAGS) -c $< -o $@

host.o: host.c protocol.h
	$(CC) $(CFLAGS) -c host.c -o $@

swctl.o: swctl.c shm_stats.h
	$(CC) $(CFLAGS) -c swctl.c -o $@

switch: switch.o $(SWITCH_OBJS)
	$(CC) $(CFLAGS) switch.o $(SWITCH_OBJS) -o $@ $(LDFLAGS)

host: host.o $(HOST_OBJS)
	$(CC) $(CFLAGS) host.o $(HOST_OBJS) -o $@ $(LDFLAGS)

swctl: swctl.o
	$(CC) $(CFLAGS) swctl.o -o $@ $(LDFLAGS)

switch_tsan: switch.c mac_table.c protocol.c protocol.h mac_table.h shm_stats.h
	$(CC) -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -g -O0 -pthread -fsanitize=thread switch.c mac_table.c protocol.c -o $@ -pthread -fsanitize=thread -lrt

host_tsan: host.c protocol.c protocol.h
	$(CC) -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -g -O0 -pthread -fsanitize=thread host.c protocol.c -o $@ -pthread -fsanitize=thread

tsan: switch_tsan host_tsan

clean:
	rm -f *.o switch host switch_tsan host_tsan swctl mac_dump.txt

.PHONY: all clean tsan
