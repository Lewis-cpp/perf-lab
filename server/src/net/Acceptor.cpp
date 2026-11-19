#include "net/Acceptor.h"
#include "net/EventLoop.h"
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

static int createListenSocket(uint16_t port) {
  int optval = 1;
  // listen_fd 的 SOCK_NONBLOCK 不会继承给 accept4() 返回的新连接
  int listen_fd =
      socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (listen_fd < 0) {
    std::perror("creat socket failed.");
    abort();
  }
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) <
      0) {
    std::perror("set reuseaddr failed.");
    abort();
  }

  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  // 主机小端序转网络大端序（16 位端口）
  addr.sin_port = htons(port);
  // INADDR_ANY (0.0.0.0)：监听本机所有网卡（lo、内网、公网）
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(listen_fd, (const sockaddr *)&addr, sizeof(addr)) < 0) {
    std::perror("bind failed.");
    abort();
  }
  return listen_fd;
}

Acceptor::Acceptor(EventLoop *loop, uint16_t port)
    : loop_(loop), listenFd_(createListenSocket(port)),
      idleFd_(open("/dev/null", O_RDONLY | O_CLOEXEC)),
      channel_(loop, listenFd_), newConnectionCallback_() {
  if (idleFd_ < 0) {
    std::perror("open file /dev/null failed.");
    abort();
  }
  channel_.setReadCallback([this] { handleRead(); });
}

Acceptor::~Acceptor() {
  // 销户铁律：先从 Poller 红黑树摘除 Channel，再关闭持有的 fd
  channel_.remove();
  ::close(listenFd_);
  if (idleFd_ >= 0) {
    ::close(idleFd_);
  }
}

void Acceptor::listen() {
  if (::listen(listenFd_, 128) < 0) {
    std::perror("listen failed.");
    abort();
  }
  if (!channel_.enableReading()) {
    std::perror("channel enable reading failed.");
    abort();
  }
}

void Acceptor::handleRead() {
  while (true) {
    // accept4 一步焊死 SOCK_NONBLOCK | SOCK_CLOEXEC（新 fd 不继承 listenFd_
    // 的非阻塞标志）
    int conn_fd =
        accept4(listenFd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (conn_fd == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        break; // 全连接队列已彻底掏空，正常退出接客循环
      }
      // 被信号打断或刚握手的连接在 accept 前被对端 RST（ECONNABORTED/EPROTO）：
      // ET 模式下绝不可提前 break，否则队列中后续正常连接会饥饿超时
      if (errno == EINTR || errno == ECONNABORTED || errno == EPROTO)
        continue;
      if (errno == EMFILE || errno == ENFILE) {
        // 【备胎救急闭环】fd 配额耗尽时，腾出 idleFd_ 坑位，用独立 tmp_fd
        // 取出队头连接立刻 close()：
        //   1) 绝不直接用 idleFd_ 覆盖保存，避免产生无人管理的“孤儿泄漏
        //   fd”且永久丢掉备胎； 2) 必须凭 accept() 的 tmp_fd >= 0 与 idleFd_ !=
        //   -1 决定是否继续，
        //      防止 accept 失败而 open 成功时陷入 100% CPU 死循环。
        if (idleFd_ != -1) {
          close(idleFd_);
          int tmp_fd = accept(listenFd_, nullptr, nullptr);
          if (tmp_fd >= 0) {
            close(tmp_fd);
          }
          idleFd_ = open("/dev/null", O_RDONLY | O_CLOEXEC);
          if (tmp_fd >= 0 && idleFd_ != -1) {
            continue;
          }
        }
        break;
      }
      std::perror("accept failed.");
      break;
    }

    if (newConnectionCallback_) {
      newConnectionCallback_(conn_fd);
    } else {
      ::close(conn_fd); // 上层未接管时立即关闭，防止 fd 泄漏
    }
  }
}
