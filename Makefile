CC ?= gcc
CFLAGS ?= -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -g -O0 -pthread
LDFLAGS ?= -pthread

SWITCH_OBJS = protocol.o mac_table.o
HOST_OBJS = protocol.o
TARGETS = switch host

all: $(TARGETS)

%.o: %.c protocol.h mac_table.h
	$(CC) $(CFLAGS) -c $< -o $@

host.o: host.c protocol.h
	$(CC) $(CFLAGS) -c host.c -o $@

switch: switch.o $(SWITCH_OBJS)
	$(CC) $(CFLAGS) switch.o $(SWITCH_OBJS) -o $@ $(LDFLAGS)

host: host.o $(HOST_OBJS)
	$(CC) $(CFLAGS) host.o $(HOST_OBJS) -o $@ $(LDFLAGS)

tsan:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(CFLAGS) -fsanitize=thread" LDFLAGS="$(LDFLAGS) -fsanitize=thread"

clean:
	rm -f *.o $(TARGETS)

.PHONY: all clean tsan
