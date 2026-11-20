// W2 阶段三验收：eventfd 跨线程唤醒 + runInLoop + 线程安全的任务队列。
//
// 构建与运行（在 server/ 目录下）：
//   cmake -S . -B build && cmake --build build --target wakeup_smoke && ./build/wakeup_smoke
//
// 这个目标用 ThreadSanitizer 而不是 ASan：阶段三的核心风险是数据竞争（pendingFunctors_ 没加锁），
// 只有 TSan 能在“碰巧没崩”的时候也报出来。TSan 和 ASan 不能同时开。
//
// 依赖的接口（阶段三要你在 EventLoop 上补齐）：
//   void runInLoop(std::function<void()>)   本线程直接执行，否则 queueInLoop
//   void queueInLoop(std::function<void()>) 任何线程可调，必要时唤醒
//   bool isInLoopThread() const
//   void quit()                             任何线程可调，不能等 poll 超时才生效
//
// 判据都是“耗时 < 100ms”：Poller 超时是 1000ms，没有唤醒就一定会等满一轮超时。
#include "net/EventLoop.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
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

using Clock = std::chrono::steady_clock;
static double msSince(Clock::time_point t0) {
  return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

static void onAlarm(int) {
  const char msg[] = "FAIL: timeout (loop never quit: a task or wakeup was lost)\n";
  (void)!write(2, msg, sizeof(msg) - 1);
  _exit(1);
}

// 1. loop 线程里 runInLoop 必须同步执行，不能拖到下一轮
static void testRunInLoopSync() {
  EventLoop loop;
  CHECK(loop.isInLoopThread());
  bool ran = false;
  loop.runInLoop([&] { ran = true; });
  CHECK(ran);
  std::printf("PASS run_in_loop_sync\n");
}

// 2. 别的线程看到的 isInLoopThread 必须是 false（否则 runInLoop 会在错误线程直接执行）
static void testNotInLoopThreadElsewhere() {
  EventLoop loop;
  bool other = true;
  std::thread t([&] { other = loop.isInLoopThread(); });
  t.join();
  CHECK(!other);
  std::printf("PASS not_in_loop_thread_elsewhere\n");
}

// 3. loop 正阻塞在 epoll_wait，别的线程投任务 → 必须被立刻敲醒
static void testCrossThreadWake() {
  EventLoop loop;
  Clock::time_point posted;
  double latency = -1;
  std::thread t([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50)); // 确保 loop 已经睡进 epoll_wait
    posted = Clock::now();
    loop.queueInLoop([&] {
      latency = msSince(posted);
      loop.quit();
    });
  });
  alarm(5);
  loop.loop();
  alarm(0);
  t.join();
  std::printf("cross-thread wake latency %.3f ms\n", latency);
  CHECK(latency >= 0 && latency < 100);
  std::printf("PASS cross_thread_wake\n");
}

// 4. 别的线程 quit() → loop 必须马上退出
static void testQuitFromOtherThread() {
  EventLoop loop;
  Clock::time_point t0;
  std::thread t([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    t0 = Clock::now();
    loop.quit();
  });
  alarm(5);
  loop.loop();
  alarm(0);
  double ms = msSince(t0);
  t.join();
  std::printf("quit-from-other-thread latency %.3f ms\n", ms);
  CHECK(ms < 100);
  std::printf("PASS quit_from_other_thread\n");
}

// 5. 任务里再排任务：阶段二这里要等一轮 1000ms 超时，阶段三必须消失
static void testNestedNoWait() {
  EventLoop loop;
  int ran = 0;
  auto t0 = Clock::now();
  loop.queueInLoop([&] {
    loop.queueInLoop([&] {
      ++ran;
      loop.quit();
    });
  });
  alarm(5);
  loop.loop();
  alarm(0);
  double ms = msSince(t0);
  std::printf("nested task latency %.3f ms\n", ms);
  CHECK(ran == 1 && ms < 100);
  std::printf("PASS nested_no_wait\n");
}

// 6. 4 个线程同时各投 20000 个任务：不能丢、不能崩、TSan 不能报竞争。
// count 故意不用 atomic：它只应该在 loop 线程被碰，TSan 报它就说明任务跑错了线程。
static void testManyProducers() {
  EventLoop loop;
  const int kThreads = 4, kPer = 20000, kTotal = kThreads * kPer;
  int count = 0;
  std::atomic<bool> wrongThread{false};
  std::vector<std::thread> ts;
  for (int i = 0; i < kThreads; ++i)
    ts.emplace_back([&] {
      for (int j = 0; j < kPer; ++j)
        loop.queueInLoop([&] {
          if (!loop.isInLoopThread()) wrongThread = true;
          if (++count == kTotal) loop.quit();
        });
    });
  auto t0 = Clock::now();
  alarm(10);
  loop.loop();
  alarm(0);
  for (auto &t : ts) t.join();
  std::printf("many producers: %d tasks in %.1f ms\n", count, msSince(t0));
  CHECK(count == kTotal && !wrongThread);
  std::printf("PASS many_producers\n");
}

int main() {
  std::signal(SIGALRM, onAlarm);
  testRunInLoopSync();
  testNotInLoopThreadElsewhere();
  testCrossThreadWake();
  testQuitFromOtherThread();
  testNestedNoWait();
  testManyProducers();
  std::printf("ALL PASS\n");
}
