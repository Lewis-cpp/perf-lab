#pragma once
#include <condition_variable>
#include <mutex>
#include <thread>

class EventLoop;

// ============================================================================
// EventLoopThread：Sub Reactor 单人工作间（One Loop Per Thread 的线程载体）
//
// 核心机制：
//   - EventLoop 在构造时定格创建者的 std::this_thread::get_id()，因此必须在子线程
//     入口 threadFunc() 的栈上构造 EventLoop，再通过条件变量把地址同步回调用方。
//
// 【所有权红线】：
//   - loop_ 仅借指子线程栈上的局部 EventLoop 对象，析构时绝不可 delete loop_；
//   - ~EventLoopThread() 持锁调用 quit() 后必须先解锁再 join()，避免子线程退出清零 loop_ 时死锁。
// ============================================================================
class EventLoopThread {
public:
  EventLoopThread();
  ~EventLoopThread();
  EventLoopThread(const EventLoopThread &other) = delete;
  EventLoopThread(EventLoopThread &&other) = delete;
  EventLoopThread &operator=(const EventLoopThread &other) = delete;
  EventLoopThread &operator=(EventLoopThread &&other) = delete;

  // 启动子线程并阻塞等待其栈上 EventLoop 构造就绪后返回指针
  EventLoop *startLoop();

private:
  void threadFunc();

  EventLoop *loop_ = nullptr; // 受 mutex_ 保护，指向子线程栈帧上的 EventLoop
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cond_;
};
