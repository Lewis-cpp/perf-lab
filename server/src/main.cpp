// W1:先写阻塞 echo，再改成 epoll LT
#include <arpa/inet.h>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

int optval = 1;

int main() {
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    std::perror("creat socket failed.");
    return -1;
  }

  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) <
      0) {
    std::perror("set reuseaddr failed.");
    return -1;
  }

  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(8888);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(listen_fd, (const sockaddr *)&addr, sizeof(addr)) < 0) {
    std::perror("bind failed.");
    return -1;
  }

  if (listen(listen_fd, 128) < 0) {
    std::perror("listen failed.");
    return -1;
  }

  while (1) {
    sockaddr_in client;
    socklen_t len = sizeof(client);
    int conn_fd = accept(listen_fd, (sockaddr *)&client, &len);
    if (conn_fd == -1) {
      perror("can't accept connection.");
      continue;
    }
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client.sin_addr, ip, sizeof(ip));
    printf("%s:%d -> fd=%d\n", ip, ntohs(client.sin_port), conn_fd);

    char buff[1024];
    while (1) {
      ssize_t n = recv(conn_fd, buff, sizeof(buff), 0);
      if (n == 0) {
        std::printf("connection closed.\n");
        break;
      }
      if (n < 0) {
        std::perror("read failed.");
        break;
      }

      ssize_t cnt = 0;
      while (cnt < n) {
        ssize_t curr_cnt = send(conn_fd, &buff[0] + cnt, n - cnt, 0);
        if (curr_cnt == -1) {
          std::perror("send failed.");
          break;
        }
        cnt += curr_cnt;
      }
    }
    close(conn_fd);
  }
}