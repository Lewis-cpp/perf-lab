// ============================================================================
// Reactor 单线程 Echo Server（基于 EventLoop / Poller / Channel / Acceptor / TcpConnection）
// ============================================================================
#include "net/Acceptor.h"
#include "net/EventLoop.h"
#include "net/TcpConnection.h"
#include <memory>
#include <unordered_map>
#include <vector>

int main() {
  EventLoop loop;
  std::unordered_map<int, std::unique_ptr<TcpConnection>> connections;
  // 轮末待回收桶：避免回调内当场 erase 销毁 TcpConnection，导致 Channel::handleEvent() 后续读悬空
  std::vector<std::unique_ptr<TcpConnection>> dying;

  Acceptor acceptor(&loop, 8888);
  acceptor.setNewConnectionCallback([&](int conn_fd) {
    std::unique_ptr<TcpConnection> conn =
        std::make_unique<TcpConnection>(&loop, conn_fd);
    // conn_fd 必须按值捕获：外层 lambda 退出后栈上的形参即销毁
    conn->setCloseCallback([&, conn_fd]() {
      auto it = connections.find(conn_fd);
      if (it == connections.end())
        return;
      // 仅首次变非空时挂一次轮末清理任务；所有同轮关闭的连接都必须无条件移入 dying 并从活跃表抹除
      if (dying.empty()) {
        loop.queueInLoop([&]() { dying.clear(); });
      }
      dying.push_back(std::move(it->second));
      connections.erase(it);
    });
    // 先挂载成功再移交所有权进账本；失败则由局部 unique_ptr 自动析构关闭 fd
    if (conn->start()) {
      connections[conn_fd] = std::move(conn);
    }
  });

  acceptor.listen();
  loop.loop();
}