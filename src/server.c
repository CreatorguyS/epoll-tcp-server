/*
 * server.c — Non-blocking, single-threaded TCP echo server using epoll
 *
 * Key design decisions (explain in Qualcomm interviews):
 *
 *  1. epoll  vs  select/poll:
 *     select/poll scan O(n) file descriptors each call.
 *     epoll uses an event-driven kernel data structure (red-black tree) that
 *     returns only *ready* descriptors — O(1) per event regardless of n.
 *     This is how production network stacks (nginx, Redis) achieve high
 *     concurrency on a single thread.
 *
 *  2. EPOLLET (edge-triggered) vs EPOLLLT (level-triggered):
 *     We use level-triggered here (default) so a partial read does not
 *     permanently stall the client. Edge-triggered is faster but demands
 *     you read/write in a loop until EAGAIN.
 *
 *  3. Framing protocol:
 *     We use a simple 4-byte big-endian length prefix so the server can
 *     handle partial reads across TCP segment boundaries — a classic pitfall.
 *
 *  4. Non-blocking sockets (O_NONBLOCK):
 *     Prevents any single slow client from blocking the event loop.
 *     EAGAIN from recv() means "no data yet" — not an error.
 *
 * Build & run:
 *   cd tcp_server
 *   gcc -O2 -Wall -o server src/server.c && ./server 8080
 *
 * Quick test (in a second terminal):
 *   python3 tests/client_test.py
 *
 * Load test:
 *   python3 tests/load_test.py --clients 500 --messages 200
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>   /* TCP_NODELAY */
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* ── Tunables ─────────────────────────────────────────────────────────── */
#define MAX_EVENTS      512          /* max epoll events per call        */
#define RECV_BUF_SIZE   4096         /* per-read buffer (stack-allocated) */
#define BACKLOG         256          /* listen() backlog                 */
#define HEADER_BYTES    4            /* length-prefix size               */
#define MAX_MSG_BYTES   (1 << 20)    /* 1 MB hard cap per message        */

/* ── Per-connection state ─────────────────────────────────────────────── */
typedef struct {
    int     fd;
    char   *read_buf;    /* dynamically grown receive buffer             */
    size_t  read_len;    /* bytes already received                       */
    size_t  read_cap;    /* allocated capacity of read_buf               */
    char   *write_buf;   /* pending outgoing bytes (echo reply)          */
    size_t  write_len;   /* bytes left to write                          */
    size_t  write_offset;
} conn_t;

/* ── Global stats (printed on SIGINT) ────────────────────────────────── */
static volatile long g_total_connections = 0;
static volatile long g_total_messages    = 0;
static volatile int  g_running           = 1;

static void on_sigint(int sig) { (void)sig; g_running = 0; }

/* ── Helpers ──────────────────────────────────────────────────────────── */

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int create_listen_socket(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    /* Avoid "Address already in use" after restart */
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    /* Disable Nagle's algorithm — reduces latency for small messages */
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_port        = htons(port),   /* host-to-network byte order */
        .sin_addr.s_addr = INADDR_ANY,
    };

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(fd); return -1;
    }
    if (listen(fd, BACKLOG) < 0) {
        perror("listen"); close(fd); return -1;
    }
    if (set_nonblocking(fd) < 0) {
        perror("fcntl"); close(fd); return -1;
    }
    return fd;
}

static conn_t *conn_new(int fd) {
    conn_t *c = calloc(1, sizeof(conn_t));
    if (!c) return NULL;
    c->fd        = fd;
    c->read_cap  = RECV_BUF_SIZE;
    c->read_buf  = malloc(c->read_cap);
    if (!c->read_buf) { free(c); return NULL; }
    return c;
}

static void conn_free(conn_t *c) {
    if (!c) return;
    close(c->fd);
    free(c->read_buf);
    free(c->write_buf);
    free(c);
}

/*
 * grow_buf — realloc buffer to at least `needed` bytes.
 * Returns 0 on success, -1 on allocation failure.
 */
static int grow_buf(char **buf, size_t *cap, size_t needed) {
    if (*cap >= needed) return 0;
    size_t new_cap = *cap * 2;
    while (new_cap < needed) new_cap *= 2;
    char *p = realloc(*buf, new_cap);
    if (!p) return -1;
    *buf = p;
    *cap = new_cap;
    return 0;
}

/* ── Accept new clients ───────────────────────────────────────────────── */

static void accept_clients(int listen_fd, int epoll_fd,
                            conn_t **conns, int max_fd) {
    while (1) {
        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);
        int cfd = accept4(listen_fd, (struct sockaddr *)&peer,
                          &peer_len, SOCK_NONBLOCK);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break; /* all done */
            perror("accept4");
            break;
        }
        if (cfd >= max_fd) {
            fprintf(stderr, "fd %d exceeds table size %d\n", cfd, max_fd);
            close(cfd);
            continue;
        }

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
        printf("[+] client fd=%d  %s:%d\n", cfd, ip, ntohs(peer.sin_port));

        conns[cfd] = conn_new(cfd);
        if (!conns[cfd]) { close(cfd); continue; }

        struct epoll_event ev = {
            .events  = EPOLLIN | EPOLLRDHUP,
            .data.fd = cfd,
        };
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cfd, &ev);
        ++g_total_connections;
    }
}

/* ── Read data from a client ─────────────────────────────────────────── */

/*
 * Protocol: each message is prefixed with a 4-byte big-endian length.
 * Server echoes the same framed message back.
 *
 * Partial reads are handled by accumulating bytes in c->read_buf until
 * a full frame is available — essential for correctness over TCP.
 */
static void handle_read(int fd, int epoll_fd, conn_t **conns) {
    conn_t *c = conns[fd];
    char tmp[RECV_BUF_SIZE];

    ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
    if (n <= 0) {
        if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
            printf("[-] client fd=%d disconnected\n", fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
            conn_free(c);
            conns[fd] = NULL;
        }
        return;
    }

    /* Append received bytes to the connection's read buffer */
    if (grow_buf(&c->read_buf, &c->read_cap, c->read_len + (size_t)n) < 0) {
        fprintf(stderr, "OOM on fd=%d\n", fd);
        goto close_conn;
    }
    memcpy(c->read_buf + c->read_len, tmp, n);
    c->read_len += (size_t)n;

    /* Consume all complete frames */
    while (c->read_len >= HEADER_BYTES) {
        /* Decode big-endian 4-byte length prefix */
        uint32_t msg_len;
        memcpy(&msg_len, c->read_buf, HEADER_BYTES);
        msg_len = ntohl(msg_len);   /* network-to-host byte order */

        if (msg_len > MAX_MSG_BYTES) {
            fprintf(stderr, "fd=%d sent oversized message (%u B)\n", fd, msg_len);
            goto close_conn;
        }

        size_t frame_len = HEADER_BYTES + msg_len;
        if (c->read_len < frame_len) break; /* incomplete frame — wait */

        /* Echo the full frame (header + payload) */
        c->write_buf    = malloc(frame_len);
        c->write_len    = frame_len;
        c->write_offset = 0;
        if (!c->write_buf) goto close_conn;
        memcpy(c->write_buf, c->read_buf, frame_len);

        /* Shift consumed bytes out of the buffer */
        memmove(c->read_buf, c->read_buf + frame_len, c->read_len - frame_len);
        c->read_len -= frame_len;

        ++g_total_messages;

        /* Register interest in EPOLLOUT to send the reply */
        struct epoll_event ev = {
            .events  = EPOLLIN | EPOLLOUT | EPOLLRDHUP,
            .data.fd = fd,
        };
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
    }
    return;

close_conn:
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    conn_free(c);
    conns[fd] = NULL;
}

/* ── Write pending echo reply ─────────────────────────────────────────── */

static void handle_write(int fd, int epoll_fd, conn_t **conns) {
    conn_t *c = conns[fd];
    while (c->write_offset < c->write_len) {
        ssize_t sent = send(fd,
                            c->write_buf + c->write_offset,
                            c->write_len  - c->write_offset,
                            MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return; /* retry later */
            goto close_conn;
        }
        c->write_offset += (size_t)sent;
    }
    free(c->write_buf);
    c->write_buf    = NULL;
    c->write_len    = 0;
    c->write_offset = 0;

    /* Done writing — stop listening for EPOLLOUT */
    struct epoll_event ev = {
        .events  = EPOLLIN | EPOLLRDHUP,
        .data.fd = fd,
    };
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
    return;

close_conn:
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    conn_free(c);
    conns[fd] = NULL;
}

/* ── Main event loop ─────────────────────────────────────────────────── */

int main(int argc, char *argv[]) {
    uint16_t port = (argc > 1) ? (uint16_t)atoi(argv[1]) : 8080;

    signal(SIGINT,  on_sigint);
    signal(SIGPIPE, SIG_IGN);   /* prevent crash on broken pipe */

    int listen_fd = create_listen_socket(port);
    if (listen_fd < 0) return 1;
    printf("[*] Listening on port %u  (PID %d)\n", port, getpid());

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) { perror("epoll_create1"); return 1; }

    struct epoll_event ev = { .events = EPOLLIN, .data.fd = listen_fd };
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

    /* Connection table indexed by file descriptor */
    int max_fd = 65536;
    conn_t **conns = calloc(max_fd, sizeof(conn_t *));
    if (!conns) { perror("calloc"); return 1; }

    struct epoll_event events[MAX_EVENTS];
    time_t last_stats = time(NULL);

    while (g_running) {
        int nev = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000 /*ms timeout*/);
        if (nev < 0 && errno != EINTR) { perror("epoll_wait"); break; }

        for (int i = 0; i < nev; ++i) {
            int efd    = events[i].data.fd;
            uint32_t ef = events[i].events;

            if (efd == listen_fd) {
                accept_clients(listen_fd, epoll_fd, conns, max_fd);
            } else {
                if (ef & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, efd, NULL);
                    conn_free(conns[efd]);
                    conns[efd] = NULL;
                    continue;
                }
                if (ef & EPOLLIN)  handle_read (efd, epoll_fd, conns);
                if (ef & EPOLLOUT && conns[efd])
                                   handle_write(efd, epoll_fd, conns);
            }
        }

        /* Print stats every 5 seconds */
        time_t now = time(NULL);
        if (now - last_stats >= 5) {
            printf("[stats] connections=%ld  messages=%ld\n",
                   g_total_connections, g_total_messages);
            last_stats = now;
        }
    }

    printf("\n[*] Shutting down — total connections=%ld  messages=%ld\n",
           g_total_connections, g_total_messages);
    free(conns);
    close(epoll_fd);
    close(listen_fd);
    return 0;
}
