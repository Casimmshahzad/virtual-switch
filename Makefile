CC ?= gcc
CFLAGS ?= -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -g -O0 -pthread
LDFLAGS ?= -pthread

SWITCH_OBJS = protocol.o mac_table.o
HOST_OBJS = protocol.o
TARGETS = switch host switch_tsan

all: switch host

%.o: %.c protocol.h mac_table.h
	$(CC) $(CFLAGS) -c $< -o $@

host.o: host.c protocol.h
	$(CC) $(CFLAGS) -c host.c -o $@

switch: switch.o $(SWITCH_OBJS)
	$(CC) $(CFLAGS) switch.o $(SWITCH_OBJS) -o $@ $(LDFLAGS)

host: host.o $(HOST_OBJS)
	$(CC) $(CFLAGS) host.o $(HOST_OBJS) -o $@ $(LDFLAGS)

switch_tsan: switch.c mac_table.c protocol.c protocol.h mac_table.h
	$(CC) -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -g -O0 -pthread -fsanitize=thread switch.c mac_table.c protocol.c -o $@ -pthread -fsanitize=thread

host_tsan: host.c protocol.c protocol.h
	$(CC) -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -g -O0 -pthread -fsanitize=thread host.c protocol.c -o $@ -pthread -fsanitize=thread

tsan: switch_tsan host_tsan
	$(MAKE) clean
	$(MAKE) CFLAGS="$(CFLAGS) -fsanitize=thread" LDFLAGS="$(LDFLAGS) -fsanitize=thread"
	cp switch switch_tsan

clean:
	rm -f *.o switch host switch_tsan host_tsan mac_dump.txt

.PHONY: all clean tsan
