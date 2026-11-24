#include "net/TcpServer.h"
#include "net/Acceptor.h"
#include "net/EventLoop.h"
#include "net/EventLoopThreadPool.h"
#include <memory>

TcpServer::TcpServer(EventLoop *loop, uint16_t port)
    : loop_(loop), acceptor_(std::make_unique<Acceptor>(loop, port)),
      threadPool_(std::make_unique<EventLoopThreadPool>(loop)) {
  acceptor_->setNewConnectionCallback([this](int fd) { newConnection(fd); });
}

TcpServer::~TcpServer() {
  loop_->assertInLoopThread();
  // 停机兜底：将仍在册的活跃连接移出账本，按值持 tmp 投回各自所属 SubLoop 完成销户与析构
  for (auto &[fd, conn] : connections_) {
    auto tmp = conn;
    conn.reset();
    tmp->getLoop()->runInLoop([tmp]() { tmp->connectDestroyed(); });
  }
}
void TcpServer::setThreadNum(size_t n) { threadPool_->setThreadNum(n); }

void TcpServer::start() {
  if (!started_) {
    started_ = true;
    // 先就绪全部 SubLoop 线程，再打开 Acceptor 监听，确保首条新连接即可正常分发
    threadPool_->start();
    acceptor_->listen();
  }
}

void TcpServer::newConnection(int fd) {
  auto ioLoop = threadPool_->getNextLoop();
  TcpConnection::TcpConnectionPtr conn =
      std::make_shared<TcpConnection>(ioLoop, fd);

  conn->setCloseCallback([this](const TcpConnection::TcpConnectionPtr &c) {
    removeConnection(c);
  });
  connections_[fd] = conn;
  // Poller 注册必须在连接所属的 ioLoop 线程内执行；若 ADD 失败则回退移除连接
  ioLoop->runInLoop([this, conn]() {
    if (!conn->start()) {
      removeConnection(conn);
    }
  });
}

void TcpServer::removeConnection(const TcpConnection::TcpConnectionPtr &conn) {
  // 第 1 跳（SubLoop -> MainLoop）：conn 必须按值捕获以延长生命周期跨越线程边界
  loop_->runInLoop([this, conn]() { removeConnectionInLoop(conn); });
}

void TcpServer::removeConnectionInLoop(
    const TcpConnection::TcpConnectionPtr &conn) {
  // 第 2 跳（MainLoop）：无锁擦除主账本，再按值捕获 conn 排回原 SubLoop 轮末收尾析构
  loop_->assertInLoopThread();
  connections_.erase(conn->fd());
  auto ioLoop = conn->getLoop();
  ioLoop->queueInLoop([conn] { conn->connectDestroyed(); });
}
