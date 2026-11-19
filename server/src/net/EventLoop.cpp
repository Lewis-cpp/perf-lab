#include "net/EventLoop.h"
#include "net/Channel.h"
#include "net/Poller.h"
#include <functional>
#include <memory>
#include <utility>
#include <vector>

EventLoop::EventLoop() : p_(std::make_unique<Poller>()) {}

// 必须在 .cpp 里定义：让 std::unique_ptr<Poller> 析构时能看到完整的 Poller 类型定义
EventLoop::~EventLoop() = default;

void EventLoop::doPendingFunctors() {
  // shortcut：执行期间新排的任务要等下一轮 poll（最多阻塞 1s），后续接 eventfd 唤醒时一并解决
  if (pendingFunctors_.empty())
    return;
  // 用 swap 换到栈局部变量，防止回调内部再调 queueInLoop() 时 vector 扩容导致迭代器失效或吞掉新任务
  std::vector<std::function<void()>> tmp;
  std::swap(tmp, pendingFunctors_);
  for (auto &task : tmp) {
    if (task)
      task();
  }
}

void EventLoop::loop() {
  while (!quit_) {
    // 每轮 poll 前必须清零 size（保留堆容量），防止重放上一轮旧指针或踩到已销毁连接的野指针
    activeChannels_.clear();
    p_->poll(activeChannels_);
    for (Channel *channel : activeChannels_) {
      channel->handleEvent();
    }
    doPendingFunctors();
  }
}

void EventLoop::quit() { quit_.store(true); }

bool EventLoop::updateChannel(Channel *channel) {
  return p_->updateChannel(channel);
}

void EventLoop::removeChannel(Channel *channel) { p_->removeChannel(channel); }

void EventLoop::queueInLoop(std::function<void()> cb) {
  pendingFunctors_.push_back(std::move(cb));
}