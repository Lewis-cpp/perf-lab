// ============================================================================
// 主从多线程 Reactor Echo Server（One Loop Per Thread：Main Loop 接客 + 4 Sub
// Loops 收发）
// ============================================================================
#include "net/EventLoop.h"
#include "net/TcpServer.h"
int main() {
  EventLoop loop;

  TcpServer server(&loop, 8888);
  server.setThreadNum(4);
  server.start();
  loop.loop();
}