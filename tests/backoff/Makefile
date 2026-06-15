PROG=		test_backoff
SRCS=		test_backoff.c

CFLAGS+=	-Wall -Wextra -std=c99 -D_DEFAULT_SOURCE
LDFLAGS+=	-lm

all: ${PROG}

${PROG}: ${SRCS}
	${CC} ${CFLAGS} -o ${PROG} ${SRCS} ${LDFLAGS}

test: ${PROG}
	@echo "Run as root: sudo ./${PROG} [-b path-to-dhcpcd]"

clean:
	rm -f ${PROG}

distclean: clean
