#include "net/TcpConnection.h"
#include "net/EventLoop.h"
#include <cerrno>
#include <cstdio>
#include <sys/socket.h>
#include <unistd.h>

TcpConnection::TcpConnection(EventLoop *loop, int fd)
    : loop_(loop), fd_(fd), channel_(loop_, fd_) {
  channel_.setReadCallback([this] { handleRead(); });
  channel_.setCloseCallback([this] { handleClose(); });
  channel_.setWriteCallback([this] { handleWrite(); });
}

TcpConnection::~TcpConnection() {
  if (!closed_)
    channel_.remove();
  ::close(fd_);
}

bool TcpConnection::start() {
  if (channel_.enableReading())
    return true;
  else {
    // 挂树失败（从未入 Poller 花名册），置 closed_ 避免析构时冗余调 remove()
    closed_ = true;
    return false;
  }
}

void TcpConnection::handleClose() {
  if (closed_)
    return;
  closed_ = true;
  // 先从 Poller 摘除，再通知上层移入轮末回收桶；fd_ 留到析构时才 close
  channel_.remove();
  if (closeCallback_)
    closeCallback_();
}

bool TcpConnection::flushOutput() {
  while (sent_ < output_.size()) {
    // MSG_NOSIGNAL：对端已断开时只返回 EPIPE，避免内核触发 SIGPIPE 杀进程
    ssize_t nwritten = send(fd(), output_.data() + sent_,
                            output_.size() - sent_, MSG_NOSIGNAL);
    if (nwritten > 0) {
      sent_ += nwritten;
      // 已发废数据超过待发数据时才一次性 erase，将内存搬移开销摊还为 O(1)
      if (sent_ > output_.size() / 2) {
        output_.erase(0, sent_);
        sent_ = 0;
      }
    } else if (nwritten == -1 && errno == EINTR) {
      continue; // ET 下写缓冲未满时被信号打断，必须原地重试
    } else if (nwritten == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      break; // 内核发送缓冲区已满，留给 afterIO() 挂 EPOLLOUT
    } else {
      std::perror("send failed.");
      handleClose();
      return false;
    }
  }
  return true;
}

void TcpConnection::afterIO(bool readPaused) {
  if (!flushOutput())
    return;
  size_t unsent = output_.size() - sent_;

  // 剩菜已 100% 发完且对端已发来 FIN：功德圆满，正式关闭连接
  if (unsent == 0 && readClosed_) {
    handleClose();
    return;
  }

  if (unsent == 0) {
    output_.clear();
    sent_ = 0;
  }

  bool wantWrite = unsent > 0;
  // 高水位背压：积压 >= 64KB 时摘读，借 TCP ZeroWindow 卡住慢客户端防 OOM
  bool wantRead = !readClosed_ && unsent < kHighWaterMark;

  if (wantWrite) {
    if (!channel_.enableWriting()) {
      handleClose();
      return;
    }
  } else {
    if (!channel_.disableWriting()) {
      handleClose();
      return;
    }
  }
  if (wantRead) {
    if (!channel_.enableReading()) {
      handleClose();
      return;
    }
  } else {
    if (!channel_.disableReading()) {
      handleClose();
      return;
    }
  }
  // 【微秒级竞态补通知 + 调度公平】：
  // 若本轮因 64KB 暂停读，但随后 flushOutput() 把积压排空导致掩码仍含读且未触发 MOD，
  // 必须显式调 update() 强制 EPOLL_CTL_MOD 让内核 ep_item_poll 复查并把本连接排到 rdllist 队尾
  if (readPaused && channel_.isReading()) {
    if (!channel_.update())
      handleClose();
  }
}

void TcpConnection::handleRead() {
  if (closed_)
    return;
  // 记账：是否未见到 EAGAIN/EOF 就因高水位提前停手（欠了一次可读通知）
  bool read_paused = false;
  char buff[1024 * 64];

  while (true) {
    // 边读边发 + 真积压判定：先 flushOutput() 尝试排空，撞上 EAGAIN 后仍 >= 64KB 才暂停读
    if (output_.size() - sent_ >= kHighWaterMark) {
      if (!flushOutput()) {
        return;
      }
      if (output_.size() - sent_ >= kHighWaterMark) {
        read_paused = true;
        break;
      }
    }

    ssize_t len = recv(fd_, buff, sizeof(buff), 0);
    if (len > 0) {
      output_.append(buff, len);
    } else if (len == 0) {
      // 对端发来 FIN：可能还有积压剩菜要发，仅标记半关闭，由 afterIO() 排空后关闭
      readClosed_ = true;
      break;
    } else {
      // ET 下遇到 EINTR 还没见到 EAGAIN，接收队列可能仍有余粮，不可 break
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break; // 内核 sk_receive_queue 已抽干

      std::perror("recv failed. ");
      handleClose();
      return;
    }
  }

  afterIO(read_paused);
}

void TcpConnection::handleWrite() {
  if (closed_)
    return;
  afterIO(false);
}