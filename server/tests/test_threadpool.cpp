// W2 阶段四（第 1 关）验收：EventLoopThread + EventLoopThreadPool（TSan）。
//
// 构建与运行（在 server/ 目录下）：
//   cmake -S . -B build && cmake --build build --target threadpool_smoke && ./build/threadpool_smoke
#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/EventLoopThreadPool.h"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <unistd.h>

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
  const char msg[] =
      "FAIL: timeout (EventLoopThread or pool hung during test)\n";
  (void)!write(2, msg, sizeof(msg) - 1);
  _exit(1);
}

// 1. 构造后未调 startLoop() 直接析构：不能对未启动的 std::thread 调 join 崩溃
static void testUnstartedDestruct() {
  {
    EventLoopThread t;
    (void)t;
  }
  std::printf("PASS unstarted_destruct\n");
}

// 2. 启动单条 IO 线程：loop 非空且归属子线程，任务在子线程执行，析构快速退出
static void testSingleThreadOwnership() {
  const std::thread::id mainTid = std::this_thread::get_id();
  std::mutex m;
  std::condition_variable cv;
  bool done = false;
  bool inLoop = false;
  std::thread::id workerTid;
  Clock::time_point destructStart;

  alarm(5);
  {
    EventLoopThread t;
    EventLoop *loop = t.startLoop();
    CHECK(loop != nullptr);
    CHECK(!loop->isInLoopThread());

    loop->runInLoop([&] {
      bool curInLoop = loop->isInLoopThread();
      std::thread::id curTid = std::this_thread::get_id();
      {
        std::lock_guard<std::mutex> lk(m);
        inLoop = curInLoop;
        workerTid = curTid;
        done = true;
        cv.notify_one();
      }
    });

    {
      std::unique_lock<std::mutex> lk(m);
      cv.wait(lk, [&] { return done; });
    }
    CHECK(inLoop);
    CHECK(workerTid != mainTid);
    destructStart = Clock::now();
  }
  alarm(0);

  double ms = msSince(destructStart);
  std::printf("single thread destruct latency %.3f ms\n", ms);
  CHECK(ms < 100.0);
  std::printf("PASS single_thread_ownership\n");
}

// 3. 析构前 loop 已提前 quit 并清零 loop_：析构期不能有数据竞争或野指针访问
static void testEarlyQuitBeforeDestruct() {
  alarm(5);
  Clock::time_point destructStart;
  {
    EventLoopThread t;
    EventLoop *loop = t.startLoop();
    CHECK(loop != nullptr);
    loop->runInLoop([loop] { loop->quit(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    destructStart = Clock::now();
  }
  alarm(0);

  double ms = msSince(destructStart);
  std::printf("early quit destruct latency %.3f ms\n", ms);
  CHECK(ms < 100.0);
  std::printf("PASS early_quit_before_destruct\n");
}

// 4. numThreads == 0：单线程退化模式，getNextLoop 恒返回 baseLoop
static void testPoolZeroThreadsFallback() {
  EventLoop baseLoop;
  EventLoopThreadPool pool(&baseLoop);
  pool.setThreadNum(0);
  pool.start();

  for (int i = 0; i < 3; ++i) {
    CHECK(pool.getNextLoop() == &baseLoop);
  }
  std::printf("PASS pool_zero_threads_fallback\n");
}

// 5. numThreads == 4：round-robin 轮转不重不漏，4000 任务全部落在各自所属线程
static void testPoolRoundRobinAndDispatch() {
  const std::thread::id mainTid = std::this_thread::get_id();
  constexpr int kThreads = 4;
  constexpr int kPerLoop = 1000;
  constexpr int kTotal = kThreads * kPerLoop;

  std::array<std::thread::id, kThreads> workerTids{};
  std::atomic<int> completed{0};
  std::atomic<bool> wrongThread{false};
  std::mutex cvMutex;
  std::condition_variable cv;
  bool allDone = false;

  alarm(10);
  {
    EventLoop baseLoop;
    EventLoopThreadPool pool(&baseLoop);
    pool.setThreadNum(kThreads);
    pool.start();

    std::array<EventLoop *, kThreads * 2> picks{};
    for (int i = 0; i < kThreads * 2; ++i) {
      picks[i] = pool.getNextLoop();
      CHECK(picks[i] != nullptr);
      CHECK(picks[i] != &baseLoop);
    }
    for (int i = 0; i < kThreads; ++i) {
      for (int j = i + 1; j < kThreads; ++j) {
        CHECK(picks[i] != picks[j]);
      }
      CHECK(picks[i + kThreads] == picks[i]);
    }

    for (int i = 0; i < kThreads; ++i) {
      EventLoop *subLoop = picks[i];
      for (int j = 0; j < kPerLoop; ++j) {
        subLoop->runInLoop([&, i, j, subLoop] {
          if (!subLoop->isInLoopThread()) {
            wrongThread.store(true, std::memory_order_relaxed);
          }
          if (j == 0) {
            workerTids[i] = std::this_thread::get_id();
          }
          if (completed.fetch_add(1, std::memory_order_acq_rel) + 1 == kTotal) {
            std::lock_guard<std::mutex> lk(cvMutex);
            allDone = true;
            cv.notify_one();
          }
        });
      }
    }

    {
      std::unique_lock<std::mutex> lk(cvMutex);
      cv.wait(lk, [&] { return allDone; });
    }

    CHECK(!wrongThread.load());
    CHECK(completed.load() == kTotal);
    for (int i = 0; i < kThreads; ++i) {
      CHECK(workerTids[i] != std::thread::id{});
      CHECK(workerTids[i] != mainTid);
      for (int j = i + 1; j < kThreads; ++j) {
        CHECK(workerTids[i] != workerTids[j]);
      }
    }
  }
  alarm(0);
  std::printf("PASS pool_round_robin_and_dispatch\n");
}

int main() {
  std::signal(SIGALRM, onAlarm);
  testUnstartedDestruct();
  testSingleThreadOwnership();
  testEarlyQuitBeforeDestruct();
  testPoolZeroThreadsFallback();
  testPoolRoundRobinAndDispatch();
  std::printf("ALL PASS\n");
}
