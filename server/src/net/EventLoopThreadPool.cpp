#include "net/EventLoopThreadPool.h"
#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>

EventLoopThreadPool::EventLoopThreadPool(EventLoop *baseLoop) {
  baseLoop_ = baseLoop;
}

// 必须在 .cpp 中定义：让 std::unique_ptr<EventLoopThread> 析构时能看到完整的类型定义
EventLoopThreadPool::~EventLoopThreadPool() = default;

void EventLoopThreadPool::setThreadNum(size_t n) {
  assert(!started_);
  numThreads_ = n;
}

void EventLoopThreadPool::start() {
  assert(!started_);
  baseLoop_->assertInLoopThread();
  started_ = true;
  for (size_t i = 0; i < numThreads_; ++i) {
    // 先通过局部 unique_ptr 启动线程拿到栈上 EventLoop*，再移交所有权进 threads_ 容器保活
    std::unique_ptr<EventLoopThread> tmp = std::make_unique<EventLoopThread>();
    this->loops_.push_back(tmp->startLoop());
    threads_.push_back(std::move(tmp));
  }
}

EventLoop *EventLoopThreadPool::getNextLoop() {
  assert(started_);
  baseLoop_->assertInLoopThread();
  // 未配置子线程（numThreads_ == 0）时直接退化为由 Main Loop 兼管连接读写
  if (loops_.empty())
    return baseLoop_;
  EventLoop *ret = loops_[next_++];
  next_ %= loops_.size();
  return ret;
}