#include "net/EventLoop.h"
#include "net/Channel.h"
#include "net/Poller.h"
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

EventLoop::EventLoop()
    : p_(std::make_unique<Poller>()), threadId_(std::this_thread::get_id()),
      wakeupFd_(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      wakeupChannel_(std::make_unique<Channel>(this, wakeupFd_)) {
  if (wakeupFd_ < 0) {
    std::perror("set wakeupFd failed.");
    abort();
  }
  wakeupChannel_->setReadCallback([this]() { handleWakeup(); });
  if (!wakeupChannel_->enableReading()) {
    std::perror("wakeupChannel enableReading() failed.");
    abort();
  }
}

// 必须在 .cpp 里定义：让 std::unique_ptr<Poller> 析构时能看到完整的 Poller 类型定义
EventLoop::~EventLoop() {
  wakeupChannel_->remove();
  ::close(wakeupFd_);
}

void EventLoop::doPendingFunctors() {
  // 锁内 O(1) swap 到栈局部变量、锁外逐个执行：防回调内重入 queueInLoop() 自死锁或迭代器失效
  std::vector<std::function<void()>> tmp;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    std::swap(tmp, pendingFunctors_);
  }
  for (auto &task : tmp) {
    if (task)
      task();
  }
}

void EventLoop::loop() {
  assertInLoopThread();
  while (!quit_) {
    // 每轮 poll 前清空 size（保留堆容量），防止重放上一轮旧指针
    activeChannels_.clear();
    p_->poll(activeChannels_);
    eventHandling_ = true;
    for (Channel *channel : activeChannels_) {
      channel->handleEvent();
    }
    eventHandling_ = false;
    doPendingFunctors();
  }
  // 退出前再收一次信箱：上一批纸条执行期间别的线程递来的任务（如 ~TcpServer 的
  // connectDestroyed）+ quit() 会让循环直接退出，不收就随 ~EventLoop 丢弃 → assert(closed_)
  doPendingFunctors();
}

void EventLoop::quit() {
  quit_.store(true);
  if (!isInLoopThread())
    wakeup();
}

bool EventLoop::updateChannel(Channel *channel) {
  assertInLoopThread();
  return p_->updateChannel(channel);
}

void EventLoop::removeChannel(Channel *channel) {
  assertInLoopThread();
  p_->removeChannel(channel);
}

void EventLoop::queueInLoop(Functor cb) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pendingFunctors_.push_back(std::move(cb));
  }
  if (!isInLoopThread() || !eventHandling_)
    wakeup();
}

void EventLoop::handleWakeup() {
  uint64_t one = 0;
  ssize_t n = read(wakeupFd_, &one, 8);
  if (n != 8 && errno != EAGAIN) {
    std::perror("handleWakeup failed.");
  }
}

void EventLoop::wakeup() {
  uint64_t one = 1;
  ssize_t n = write(wakeupFd_, &one, 8);
  if (n != 8) {
    std::perror("wakeup() func failed.");
  }
}

bool EventLoop::isInLoopThread() const {
  return std::this_thread::get_id() == threadId_;
}

void EventLoop::assertInLoopThread() const {
  if (!isInLoopThread()) {
    std::fprintf(stderr, "EventLoop::assertInLoopThread failed!\n");
    abort();
  }
}

void EventLoop::runInLoop(Functor cb) {
  if (!cb)
    return;
  if (isInLoopThread())
    cb();
  else {
    queueInLoop(std::move(cb));
  }
}
