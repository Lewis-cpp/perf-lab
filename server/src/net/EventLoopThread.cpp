#include "net/EventLoopThread.h"
#include "net/EventLoop.h"
#include <cassert>
#include <cstddef>
#include <mutex>
#include <thread>

EventLoopThread::EventLoopThread() = default;
EventLoopThread::~EventLoopThread() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (loop_ != nullptr)
      loop_->quit();
  }
  // 必须先解锁再 join：否则子线程退出 loop() 后获取 mutex_ 清零 loop_ 时会与本线程互锁
  if (thread_.joinable()) {
    thread_.join();
  }
}

EventLoop *EventLoopThread::startLoop() {
  assert(!thread_.joinable() && "thread_ already run task.");
  thread_ = std::thread(&EventLoopThread::threadFunc, this);

  EventLoop *tmp;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    // 等待子线程在自己的栈上完成 EventLoop 构造并写入 loop_
    cond_.wait(lock, [this] { return loop_ != nullptr; });
    tmp = loop_;
  }
  return tmp;
}

void EventLoopThread::threadFunc() {
  // 在子线程栈上构造，使 EventLoop::threadId_ 绑定为当前子线程
  EventLoop loop;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = &loop;
  }

  cond_.notify_one();
  loop.loop();

  // 栈上 loop 即将随函数返回销毁，持锁擦除指针防止析构期悬垂访问
  {
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
  }
}
