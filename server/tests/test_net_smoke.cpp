// net 库冒烟测试：不依赖 socket，用两根非阻塞 pipe 驱动 EventLoop / Channel / Poller。
//
// 构建与运行（在 server/ 目录下）：
//   cmake -S . -B build && cmake --build build --target net_smoke && ./build/net_smoke
//
// 场景（ET 下，一次写入只应触发一次读回调）：
//   第 1 轮：只有 pipe A 可读 → A 的回调读走字节，再往 pipe B 写 1 字节
//   第 2 轮：只有 pipe B 可读 → B 的回调调用 loop.quit()
// 期望：A 的回调恰好执行 1 次。若某轮没清空 activeChannels_，第 2 轮会把 A 再分发一遍，计数会变成 2。
//
// queueInLoop 两个用例：
//   pending_after_events：A、B 同一轮就绪，A 的回调排一个任务 → 任务必须在 A、B 都处理完之后才执行
//   nested_queue：任务里再排 100 个任务（迫使 vector 扩容）→ 不能踩迭代器，也不能丢任务
#include "net/Channel.h"
#include "net/EventLoop.h"

#include <chrono>
#include <csignal>
#include <cstdio>
#include <string>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);     \
      std::exit(1);                                                            \
    }                                                                          \
  } while (0)

// loop 永不退出（任务丢失 / 事件丢失）时，用 alarm 给出明确的失败信息而不是挂死
static void onAlarm(int) {
  const char msg[] = "FAIL: timeout (loop never quit: a task or event was lost)\n";
  (void)!write(2, msg, sizeof(msg) - 1);
  _exit(1);
}

static void testDispatchOnce() {
  int a[2], b[2];
  CHECK(pipe2(a, O_NONBLOCK | O_CLOEXEC) == 0);
  CHECK(pipe2(b, O_NONBLOCK | O_CLOEXEC) == 0);

  EventLoop loop;
  Channel chA(&loop, a[0]), chB(&loop, b[0]);
  int countA = 0, countB = 0;
  char c;

  chA.setReadCallback([&] {
    ++countA;
    while (read(a[0], &c, 1) > 0) {} // ET：读到 EAGAIN
    CHECK(write(b[1], "y", 1) == 1);
  });
  chB.setReadCallback([&] {
    ++countB;
    while (read(b[0], &c, 1) > 0) {}
    loop.quit();
  });

  CHECK(chA.enableReading()); // 首次走 ADD
  CHECK(chB.enableReading());
  CHECK(chA.enableReading()); // 位没变：应直接返回 true，不发 MOD

  CHECK(write(a[1], "x", 1) == 1);
  loop.loop();

  std::printf("countA=%d countB=%d\n", countA, countB);
  CHECK(countA == 1);
  CHECK(countB == 1);

  // 先摘树再关 fd（顺序反了会留下 fd 复用隐患）
  chA.remove();
  chB.remove();
  for (int fd : {a[0], a[1], b[0], b[1]}) close(fd);
  std::printf("PASS dispatch_once\n");
}

// 同一轮就绪的 A、B：A 的回调排队的任务必须在本轮所有 handleEvent 之后才执行
static void testPendingAfterEvents() {
  int a[2], b[2];
  CHECK(pipe2(a, O_NONBLOCK | O_CLOEXEC) == 0);
  CHECK(pipe2(b, O_NONBLOCK | O_CLOEXEC) == 0);
  EventLoop loop;
  Channel chA(&loop, a[0]), chB(&loop, b[0]);
  std::string log;
  char c;
  chA.setReadCallback([&] {
    while (read(a[0], &c, 1) > 0) {}
    log += 'A';
    loop.queueInLoop([&] { log += 'F'; loop.quit(); });
  });
  chB.setReadCallback([&] {
    while (read(b[0], &c, 1) > 0) {}
    log += 'B';
  });
  CHECK(chA.enableReading());
  CHECK(chB.enableReading());
  CHECK(write(a[1], "x", 1) == 1); // loop 启动前两边都写好 → 同一次 epoll_wait 返回两个
  CHECK(write(b[1], "y", 1) == 1);
  loop.loop();

  std::printf("log=%s\n", log.c_str());
  CHECK(log.size() == 3 && log[2] == 'F'); // "ABF" 或 "BAF"，绝不能是 "AFB"
  chA.remove();
  chB.remove();
  for (int fd : {a[0], a[1], b[0], b[1]}) close(fd);
  std::printf("PASS pending_after_events\n");
}

// 任务执行时再排任务：100 个足以让 vector 多次扩容。
// 若 doPendingFunctors 直接遍历成员 vector，push_back 扩容会让迭代器悬空（ASan 报 heap-use-after-free）
static void testNestedQueue() {
  int a[2];
  CHECK(pipe2(a, O_NONBLOCK | O_CLOEXEC) == 0);
  EventLoop loop;
  Channel chA(&loop, a[0]);
  const int kNested = 100;
  int ran = 0;
  char c;
  chA.setReadCallback([&] {
    while (read(a[0], &c, 1) > 0) {}
    loop.queueInLoop([&] {
      for (int i = 0; i < kNested; ++i)
        loop.queueInLoop([&] {
          if (++ran == kNested) loop.quit();
        });
    });
  });
  CHECK(chA.enableReading());
  CHECK(write(a[1], "x", 1) == 1);

  auto t0 = std::chrono::steady_clock::now();
  alarm(5);
  loop.loop();
  alarm(0);
  double ms = std::chrono::duration<double, std::milli>(
                  std::chrono::steady_clock::now() - t0).count();

  CHECK(ran == kNested);
  // 嵌套任务要等下一轮：下一轮先阻塞在 poll（无事件时 1000ms 超时）。这是已知的捷径，阶段三用 eventfd 解决
  std::printf("nested tasks ran=%d, took %.0f ms (expect ~1000ms: waited one poll timeout)\n", ran, ms);
  chA.remove();
  for (int fd : {a[0], a[1]}) close(fd);
  std::printf("PASS nested_queue\n");
}

int main() {
  std::signal(SIGALRM, onAlarm);
  testDispatchOnce();
  testPendingAfterEvents();
  testNestedQueue();
  std::printf("ALL PASS\n");
}
