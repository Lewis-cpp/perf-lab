// W2 阶段四终极验收：TcpServer 主从多线程并发收发、三跳关闭与停机回收（TSan）。
//
// 构建与运行（在 server/ 目录下）：
//   cmake -S . -B build && cmake --build build --target tcpserver_smoke && ./build/tcpserver_smoke
#include "net/EventLoop.h"
#include "net/TcpServer.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);     \
      std::exit(1);                                                            \
    }                                                                          \
  } while (0)

static void onAlarm(int) {
  const char msg[] = "FAIL: timeout in tcpserver_smoke\n";
  (void)!write(2, msg, sizeof(msg) - 1);
  _exit(1);
}

static int countProcessThreads() {
  DIR *dir = opendir("/proc/self/task");
  CHECK(dir != nullptr);
  int n = 0;
  while (readdir(dir) != nullptr) {
    ++n;
  }
  closedir(dir);
  return n - 2; // 减去 "." 和 ".."
}

static int connectLocal(uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  CHECK(fd >= 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  CHECK(::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);
  CHECK(::connect(fd, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) ==
        0);
  return fd;
}

static void sendAll(int fd, const std::string &msg) {
  size_t sent = 0;
  while (sent < msg.size()) {
    ssize_t n = ::send(fd, msg.data() + sent, msg.size() - sent, MSG_NOSIGNAL);
    CHECK(n > 0);
    sent += static_cast<size_t>(n);
  }
}

static std::string recvExact(int fd, size_t len) {
  std::string out(len, '\0');
  size_t got = 0;
  while (got < len) {
    ssize_t n = ::recv(fd, &out[got], len - got, 0);
    CHECK(n > 0);
    got += static_cast<size_t>(n);
  }
  return out;
}

// 1. 4 个 Sub Reactor：核验 OS 线程数、32 客户端并发收发 + 半数主动断开（三跳关闭）+ 半数保留至 ~TcpServer 停机回收
static void testMultiReactorEchoAndShutdown() {
  constexpr uint16_t kPort = 18888;
  constexpr int kSubLoops = 4;
  constexpr int kClients = 32;

  std::mutex readyMtx;
  std::condition_variable readyCv;
  bool serverReady = false;
  EventLoop *mainLoopPtr = nullptr;

  std::thread serverThread([&] {
    EventLoop loop;
    TcpServer server(&loop, kPort);
    server.setThreadNum(kSubLoops);
    server.start();

    {
      std::lock_guard<std::mutex> lk(readyMtx);
      mainLoopPtr = &loop;
      serverReady = true;
      readyCv.notify_one();
    }
    loop.loop();
  });

  {
    std::unique_lock<std::mutex> lk(readyMtx);
    readyCv.wait(lk, [&] { return serverReady; });
  }

  // 主线程 + serverThread(MainLoop) + 4 SubLoops >= 6 个线程（TSan 自身可能另起后台线程）
  CHECK(countProcessThreads() >= 1 + 1 + kSubLoops);

  // 前 24 个客户端：并发完成多轮大块回显后主动 close（压力测试三跳关闭）
  std::vector<std::thread> clientThreads;
  for (int i = 0; i < 24; ++i) {
    clientThreads.emplace_back([i] {
      int fd = connectLocal(kPort);
      std::string payload(8192, static_cast<char>('A' + (i % 26)));
      for (int round = 0; round < 5; ++round) {
        sendAll(fd, payload);
        CHECK(recvExact(fd, payload.size()) == payload);
      }
      ::close(fd);
    });
  }
  for (auto &t : clientThreads) {
    t.join();
  }

  // 后 8 个客户端：保持长连接不主动断开，测试 ~TcpServer() 跨线程停机兜底清理
  std::vector<int> lingeringFds;
  for (int i = 0; i < kClients - 24; ++i) {
    int fd = connectLocal(kPort);
    std::string ping = "lingering_" + std::to_string(i);
    sendAll(fd, ping);
    CHECK(recvExact(fd, ping.size()) == ping);
    lingeringFds.push_back(fd);
  }

  // 给前一批三跳关闭留足排空时间，随后退出 MainLoop 触发 ~TcpServer()
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  mainLoopPtr->quit();
  serverThread.join();

  for (int fd : lingeringFds) {
    ::close(fd);
  }
  std::printf("PASS multi_reactor_echo_and_shutdown\n");
}

// 2. 0 线程单 Reactor 退化模式：同样支持正常收发与停机活跃连接清理
static void testZeroThreadMode() {
  constexpr uint16_t kPort = 18889;
  std::mutex readyMtx;
  std::condition_variable readyCv;
  bool serverReady = false;
  EventLoop *mainLoopPtr = nullptr;

  std::thread serverThread([&] {
    EventLoop loop;
    TcpServer server(&loop, kPort);
    server.setThreadNum(0);
    server.start();
    {
      std::lock_guard<std::mutex> lk(readyMtx);
      mainLoopPtr = &loop;
      serverReady = true;
      readyCv.notify_one();
    }
    loop.loop();
  });

  {
    std::unique_lock<std::mutex> lk(readyMtx);
    readyCv.wait(lk, [&] { return serverReady; });
  }

  int fd1 = connectLocal(kPort);
  sendAll(fd1, "hello_single");
  CHECK(recvExact(fd1, 12) == "hello_single");
  ::close(fd1);

  int fd2 = connectLocal(kPort);
  sendAll(fd2, "stay_open");
  CHECK(recvExact(fd2, 9) == "stay_open");

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  mainLoopPtr->quit();
  serverThread.join();
  ::close(fd2);
  std::printf("PASS zero_thread_mode\n");
}

int main() {
  std::signal(SIGALRM, onAlarm);
  alarm(15);
  testMultiReactorEchoAndShutdown();
  testZeroThreadMode();
  alarm(0);
  std::printf("ALL PASS\n");
}
