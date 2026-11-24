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
#include <cstdlib>
#include <fcntl.h>
#include <memory>
#include <string>
#include <sys/socket.h>
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

  // 阶段三起：doPendingFunctors 期间排的任务会 wakeup()，下一轮 poll 立刻返回，不再等 1000ms 超时
  std::printf("nested tasks ran=%d, took %.1f ms (expect <100ms: eventfd wakeup)\n", ran, ms);
  CHECK(ran == kNested && ms < 100);
  chA.remove();
  for (int fd : {a[0], a[1]}) close(fd);
  std::printf("PASS nested_queue\n");
}

// Channel::tie 生命周期守卫：
// A) readCallback 内释放唯一外部 shared_ptr 时，Channel 栈上 guard 保活到同轮 writeCallback 结束（ASan 零 UAF）
// B) 绑定对象在 handleEvent 前已销毁时，tie_.lock() 失败应直接跳过回调
static void testChannelTie() {
  // Case A
  {
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                     sv) == 0);

    struct Owner {
      Channel ch;
      bool *destroyedFlag;
      Owner(EventLoop *loop, int fd, bool *flag)
          : ch(loop, fd), destroyedFlag(flag) {}
      ~Owner() { *destroyedFlag = true; }
    };

    EventLoop loop;
    bool destroyed = false;
    bool writeRan = false;
    auto owner = std::make_shared<Owner>(&loop, sv[0], &destroyed);
    owner->ch.tie(owner);

    owner->ch.setReadCallback([&] {
      char buf[64];
      while (recv(sv[0], buf, sizeof(buf), 0) > 0) {}
      owner->ch.remove();
      owner.reset();
      CHECK(!destroyed);
    });
    owner->ch.setWriteCallback([&] {
      CHECK(!destroyed);
      writeRan = true;
      loop.quit();
    });

    CHECK(owner->ch.enableReading());
    CHECK(owner->ch.enableWriting());
    CHECK(write(sv[1], "x", 1) == 1);

    alarm(5);
    loop.loop();
    alarm(0);

    CHECK(writeRan && destroyed);
    close(sv[0]);
    close(sv[1]);
  }

  // Case B
  {
    int p[2];
    CHECK(pipe2(p, O_NONBLOCK | O_CLOEXEC) == 0);
    EventLoop loop;
    Channel ch(&loop, p[0]);
    auto token = std::make_shared<int>(42);
    ch.tie(token);

    int ranCount = 0;
    ch.setReadCallback([&] { ++ranCount; });
    CHECK(ch.enableReading());
    CHECK(write(p[1], "x", 1) == 1);
    token.reset();

    loop.queueInLoop([&] { loop.quit(); });
    alarm(5);
    loop.loop();
    alarm(0);

    CHECK(ranCount == 0);
    ch.remove();
    close(p[0]);
    close(p[1]);
  }

  std::printf("PASS channel_tie\n");
}

int main() {
  std::signal(SIGALRM, onAlarm);
  testDispatchOnce();
  testPendingAfterEvents();
  testNestedQueue();
  testChannelTie();
  std::printf("ALL PASS\n");
}
