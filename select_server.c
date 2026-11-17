#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>

#define PORT 5555
#define MAX_CLIENTS 5
#define BUF_SIZE 4096

typedef struct {
    int fd;
    char buffer[BUF_SIZE];
    size_t len;
} Client;

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void close_client(Client *c) {
    if (c->fd != -1) {
        printf("Client disconnected: fd=%d\n", c->fd);
        close(c->fd);
        c->fd = -1;
        c->len = 0;
    }
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

    Client clients[MAX_CLIENTS];
    for (int i = 0; i < MAX_CLIENTS; i++) {
        clients[i].fd = -1;
        clients[i].len = 0;
    }

    printf("select server listening on port %d\n", PORT);

    while (1) {
        fd_set reads, writes;
        FD_ZERO(&reads);
        FD_ZERO(&writes);

        FD_SET(server_fd, &reads);
        int maxfd = server_fd;

        for (int i = 0; i < MAX_CLIENTS; i++) {
            int fd = clients[i].fd;
            if (fd == -1) continue;

            if (clients[i].len == 0)
                FD_SET(fd, &reads);
            else
                FD_SET(fd, &writes);

            if (fd > maxfd) maxfd = fd;
        }

        int ready = select(maxfd + 1, &reads, &writes, NULL, NULL);

        if (ready == -1) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        if (FD_ISSET(server_fd, &reads)) {
            while (1) {
                int fd = accept(server_fd, NULL, NULL);

                if (fd == -1) {
                    if (errno == EINTR) continue;
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    perror("accept");
                    break;
                }

                if (fd >= FD_SETSIZE || set_nonblocking(fd) == -1) {
                    close(fd);
                    continue;
                }

                int slot = -1;
                for (int i = 0; i < MAX_CLIENTS; i++) {
                    if (clients[i].fd == -1) {
                        slot = i;
                        break;
                    }
                }

                if (slot == -1) {
                    printf("Maximum clients reached\n");
                    close(fd);
                } else {
                    clients[slot].fd = fd;
                    clients[slot].len = 0;
                    printf("Client connected: fd=%d\n", fd);
                }
            }
        }

        for (int i = 0; i < MAX_CLIENTS; i++) {
            Client *c = &clients[i];
            int fd = c->fd;

            if (fd == -1) continue;

            if (FD_ISSET(fd, &reads)) {
                ssize_t n = recv(fd, c->buffer, sizeof(c->buffer), 0);

                if (n > 0) {
                    c->len = (size_t)n;
                    printf("Received %zd bytes from fd=%d\n", n, fd);
                } else if (n == 0) {
                    close_client(c);
                    continue;
                } else if (errno != EAGAIN &&
                           errno != EWOULDBLOCK &&
                           errno != EINTR) {
                    perror("recv");
                    close_client(c);
                    continue;
                }
            }

            if (c->fd != -1 && FD_ISSET(fd, &writes)) {
                ssize_t n = send(fd, c->buffer, c->len, MSG_NOSIGNAL);

                if (n > 0) {
                    if ((size_t)n < c->len) {
                        memmove(c->buffer, c->buffer + n, c->len - (size_t)n);
                    }
                    c->len -= (size_t)n;
                } else if (n == -1 && errno != EAGAIN &&
                           errno != EWOULDBLOCK && errno != EINTR) {
                    perror("send");
                    close_client(c);
                }
            }
        }
    }

    for (int i = 0; i < MAX_CLIENTS; i++)
        close_client(&clients[i]);

    close(server_fd);
    return 0;
}
