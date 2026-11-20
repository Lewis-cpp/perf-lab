#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

// 前向声明即可：完整定义留到 EventLoop.cpp；
// 因 p_ / wakeupChannel_ 为 std::unique_ptr，构造与析构必须在 .cpp 中 out-of-line 定义。
class Poller;
class Channel;

// ============================================================================
// EventLoop：事件驱动心脏 / 火锅店“大堂经理”（One Loop Per Thread 核心）
//
// 【所有权与线程边界红线】：
//   - 独占拥有唯一的 Poller (p_) 与跨线程唤醒门铃 (wakeupFd_ / wakeupChannel_)；
//   - 仅借用 activeChannels_ 里的 Channel*，不负责外部连接的生命周期；
//   - Poller 红黑树与花名册完全无锁，只能在所属线程 (threadId_) 内操作；
//     跨线程交互一律走 runInLoop / queueInLoop + wakeup() 排队通知所属线程执行。
// ============================================================================
class EventLoop {
public:
  using Functor = std::function<void()>;

  EventLoop();
  ~EventLoop();

  // 禁止拷贝与移动：绑定所属线程 threadId_ 且独占内核资源 (wakeupFd_ / epfd_)
  EventLoop(const EventLoop &e) = delete;
  EventLoop(EventLoop &&e) = delete;
  EventLoop &operator=(const EventLoop &e) = delete;
  EventLoop &operator=(EventLoop &&e) = delete;

  // -------------------- 1. 主循环生命周期 --------------------
  // 启动主循环：清空 activeChannels_ -> Poller::poll -> handleEvent() -> doPendingFunctors()
  void loop();

  // 置 quit_ = true，优雅停机；若不在所属线程调用则唤醒循环，避免干等 poll 超时
  void quit();

  // -------------------- 2. 线程归属判定与防呆护栏 --------------------
  // 任意线程可安全调用：判断当前调用线程是否为创建本 EventLoop 的所属线程
  bool isInLoopThread() const;

  // 单线程禁区护栏：供 loop / updateChannel / removeChannel 等入口检查归属，越界立即报错退出
  void assertInLoopThread() const;

  // -------------------- 3. 跨线程任务投递与门铃唤醒 --------------------
  // 任意线程可调：若在所属线程则同步执行 cb；否则转交 queueInLoop()
  void runInLoop(Functor cb);

  // 任意线程可调：加锁入队 pendingFunctors_，若不在免唤醒安全窗口内则敲响 wakeup() 门铃
  void queueInLoop(Functor cb);

  // 往 wakeupFd_ 写入唤醒信号，立刻敲醒阻塞在 Poller::poll() 的事件循环
  void wakeup();

  // -------------------- 4. 转发给内部 Poller（仅限所属线程调用） --------------------
  [[nodiscard]] bool updateChannel(Channel *channel);
  void removeChannel(Channel *channel);

private:
  // 读空 wakeupFd_（在全局 EPOLLET 契约下需排空至 EAGAIN）
  void handleWakeup();

  // 执行本轮待办任务：加锁 swap 出队列后在锁外逐个执行（防重入死锁与迭代器失效）
  void doPendingFunctors();

  // 【声明顺序红线】（成员按以下顺序构造，析构时按逆序销毁）：
  //   1) p_ 必须先于 wakeupChannel_ 声明，保证构造函数中挂树与析构阶段销户顺序安全；
  //   2) threadId_ 必须先于 wakeupChannel_ 初始化，确保构造体内 enableReading()
  //      触发 assertInLoopThread() 时 threadId_ 已就绪；
  //   3) wakeupFd_ 必须排在 wakeupChannel_ 之前，保证初始化列表构造 Channel 时 fd 已有效。
  std::unique_ptr<Poller> p_;
  const std::thread::id threadId_;

  // 优雅停机开关（替代 while(1)）
  std::atomic<bool> quit_ = false;
  // 唤醒窗口状态位：仅在分发 activeChannels_ 执行 handleEvent() 期间为 true
  // （此时紧接着必执行 doPendingFunctors()，本线程入队可免一次 wakeup()；其余时刻均需唤醒）
  std::atomic<bool> eventHandling_ = false;

  // 跨线程唤醒门铃（eventfd 8 字节计数器，读写共用 1 个 fd）
  int wakeupFd_;
  std::unique_ptr<Channel> wakeupChannel_;

  // 每轮复用的活跃工牌名单：clear() 保留容量，稳态零堆分配
  std::vector<Channel *> activeChannels_;

  // 全类唯一需要加锁保护的共享队列（保护 pendingFunctors_ 的跨线程入队与本轮 swap）
  std::mutex mutex_;
  std::vector<Functor> pendingFunctors_;
};
