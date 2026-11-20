CC ?= gcc
CFLAGS ?= -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -g -O0 -pthread
LDFLAGS ?= -pthread

COMMON_OBJS = protocol.o
TARGETS = switch host

all: $(TARGETS)

%.o: %.c protocol.h
	$(CC) $(CFLAGS) -c $< -o $@

switch: switch.o $(COMMON_OBJS)
	$(CC) $(CFLAGS) switch.o $(COMMON_OBJS) -o $@ $(LDFLAGS)

host: host.o $(COMMON_OBJS)
	$(CC) $(CFLAGS) host.o $(COMMON_OBJS) -o $@ $(LDFLAGS)

tsan:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(CFLAGS) -fsanitize=thread" LDFLAGS="$(LDFLAGS) -fsanitize=thread"

clean:
	rm -f *.o $(TARGETS)

.PHONY: all clean tsan
