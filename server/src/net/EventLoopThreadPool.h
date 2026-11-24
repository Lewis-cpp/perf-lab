#pragma once

#include <cstddef>
#include <memory>
#include <vector>
class EventLoop;
class EventLoopThread;

// ============================================================================
// EventLoopThreadPool：Sub Reactor IO 线程池（分楼层经理排班表）
//
// 职责与线程契约：
//   - 由 Main Reactor (baseLoop_) 独占驱动，start() 与 getNextLoop() 均只在主线程调用，无需加锁；
//   - 按 round-robin 轮询为新连接指派 Sub Loop；当 numThreads_ == 0 时退化返回 baseLoop_（单线程模式）。
//
// 【所有权红线】：
//   - threads_ (unique_ptr) 独占管理各 IO 线程生命周期（析构自动 quit + join）；
//   - loops_ 仅缓存各子线程栈上 EventLoop 的裸指针名片，绝不可 delete；
//   - 因 EventLoopThread 为前向声明，~EventLoopThreadPool() 必须在 .cpp 中定义。
// ============================================================================
class EventLoopThreadPool {
public:
  explicit EventLoopThreadPool(EventLoop *baseLoop);
  ~EventLoopThreadPool();
  EventLoopThreadPool(const EventLoopThreadPool &other) = delete;
  EventLoopThreadPool(EventLoopThreadPool &&other) = delete;
  EventLoopThreadPool &operator=(const EventLoopThreadPool &other) = delete;
  EventLoopThreadPool &operator=(EventLoopThreadPool &&other) = delete;

  // 必须在 start() 前调用；设为 0 表示单 Reactor 单线程模式
  void setThreadNum(size_t n);
  void start();
  // 轮询返回下一个处理连接 I/O 的 EventLoop*
  EventLoop *getNextLoop();

private:
  EventLoop *baseLoop_;
  bool started_ = false;
  size_t numThreads_ = 0;
  std::size_t next_ = 0;
  std::vector<std::unique_ptr<EventLoopThread>> threads_;
  std::vector<EventLoop *> loops_;
};