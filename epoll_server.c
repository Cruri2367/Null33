#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>

#define PORT 5555
#define MAX_EVENTS 64

#define USE_ET 0

typedef struct {
    int fd;
    char *data;
    size_t len;
    size_t cap;
} Client;

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void close_client(int ep, Client *c) {
    if (!c) return;

    printf("Client disconnected: fd=%d\n", c->fd);
    epoll_ctl(ep, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    free(c->data);
    free(c);
}

static int update_client(int ep, Client *c) {
    struct epoll_event ev = {0};

    ev.events = EPOLLIN | EPOLLRDHUP;
#if USE_ET
    ev.events |= EPOLLET;
#endif
    if (c->len > 0)
        ev.events |= EPOLLOUT;

    ev.data.ptr = c;

    return epoll_ctl(ep, EPOLL_CTL_MOD, c->fd, &ev);
}

static int append_data(Client *c, const char *buf, size_t n) {
    if (n > SIZE_MAX - c->len) return -1;

    size_t needed = c->len + n;

    if (needed > c->cap) {
        size_t new_cap = c->cap ? c->cap : 4096;
        while (new_cap < needed) {
            if (new_cap > SIZE_MAX / 2) {
                new_cap = needed;
                break;
            }
            new_cap *= 2;
        }

        char *p = realloc(c->data, new_cap);
        if (!p) return -1;

        c->data = p;
        c->cap = new_cap;
    }

    memcpy(c->data + c->len, buf, n);
    c->len += n;
    return 0;
}

static int read_client(Client *c) {
    char buf[4096];

    while (1) {
        ssize_t n = recv(c->fd, buf, sizeof(buf), 0);

        if (n > 0) {
            if (append_data(c, buf, (size_t)n) == -1)
                return -1;
            continue;
        }

        if (n == 0)
            return 0;

        if (errno == EINTR)
            continue;

        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return 1;

        return -1;
    }
}

static int write_client(Client *c) {
    while (c->len > 0) {
        ssize_t n = send(c->fd, c->data, c->len, MSG_NOSIGNAL);

        if (n > 0) {
            c->len -= (size_t)n;
            if (c->len > 0)
                memmove(c->data, c->data + n, c->len);
            continue;
        }

        if (n == -1 && errno == EINTR)
            continue;

        if (n == -1 &&
            (errno == EAGAIN || errno == EWOULDBLOCK))
            return 1;

        return -1;
    }

    return 0;
}

int main(void) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1 ||
        listen(server_fd, 16) == -1 ||
        set_nonblocking(server_fd) == -1) {
        perror("server setup");
        close(server_fd);
        return 1;
    }

    int ep = epoll_create1(EPOLL_CLOEXEC);
    if (ep == -1) {
        perror("epoll_create1");
        close(server_fd);
        return 1;
    }

    struct epoll_event ev = {0};
    ev.events = EPOLLIN;
#if USE_ET
    ev.events |= EPOLLET;
#endif
    ev.data.ptr = NULL;

    if (epoll_ctl(ep, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
        perror("epoll_ctl");
        close(ep);
        close(server_fd);
        return 1;
    }

#if USE_ET
    printf("epoll ET server listening on port %d\n", PORT);
#else
    printf("epoll LT server listening on port %d\n", PORT);
#endif

    struct epoll_event events[MAX_EVENTS];

    while (1) {
        int count = epoll_wait(ep, events, MAX_EVENTS, -1);

        if (count == -1) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < count; i++) {
            if (events[i].data.ptr == NULL) {
                while (1) {
                    int fd = accept(server_fd, NULL, NULL);

                    if (fd == -1) {
                        if (errno == EINTR) continue;
                        if (errno == EAGAIN ||
                            errno == EWOULDBLOCK) break;
                        perror("accept");
                        break;
                    }

                    if (set_nonblocking(fd) == -1) {
                        close(fd);
                        continue;
                    }

                    Client *c = calloc(1, sizeof(Client));
                    if (!c) {
                        close(fd);
                        continue;
                    }

                    c->fd = fd;

                    struct epoll_event client_ev = {0};
                    client_ev.events = EPOLLIN | EPOLLRDHUP;
#if USE_ET
                    client_ev.events |= EPOLLET;
#endif
                    client_ev.data.ptr = c;

                    if (epoll_ctl(ep, EPOLL_CTL_ADD,
                                  fd, &client_ev) == -1) {
                        perror("epoll_ctl ADD");
                        close(fd);
                        free(c);
                        continue;
                    }

                    printf("Client connected: fd=%d\n", fd);
                }

                continue;
            }

            Client *c = events[i].data.ptr;
            uint32_t flags = events[i].events;
            int close_now = 0;

            if (flags & (EPOLLIN | EPOLLRDHUP |
                         EPOLLHUP | EPOLLERR)) {
                int r = read_client(c);
                if (r <= 0)
                    close_now = 1;
            }

            if (!close_now && c->len > 0) {
                int r = write_client(c);
                if (r < 0)
                    close_now = 1;
            }

            if (!close_now && (flags & EPOLLERR))
                close_now = 1;

            if (close_now) {
                close_client(ep, c);
                continue;
            }

            if (update_client(ep, c) == -1) {
                perror("epoll_ctl MOD");
                close_client(ep, c);
            }
        }
    }

    close(ep);
    close(server_fd);
    return 0;
}
