#include "net/Channel.h"
#include "net/EventLoop.h" // 头文件仅有前向声明，调用成员需完整定义
#include <cstdint>
#include <memory>
#include <sys/epoll.h>
#include <utility>

bool Channel::update() { return loop_->updateChannel(this); }

void Channel::remove() { loop_->removeChannel(this); }

void Channel::handleEventWithGuard() {
  // 对端挂断且无可读数据（如背压关读期间收到 RST）：直接走关闭回调返回
  if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
    if (closeCallback_)
      closeCallback_();
    return;
  }

  // 可读 / 半关闭 (EPOLLRDHUP) / 出错合并为单次 readCallback_()，一轮只进一次读循环
  if (revents_ & EPOLLIN || revents_ & EPOLLRDHUP || revents_ & EPOLLERR) {
    if (readCallback_)
      readCallback_();
  }

  if (revents_ & EPOLLOUT) {
    if (writeCallback_)
      writeCallback_();
  }
}
// 若绑定过宿主生命周期 (tied_)，先用 tie_.lock() 在栈上持有临时强引用保活至本函数退出；
// 若宿主已提前销毁 (!tmp) 则直接跳过事件分发。
void Channel::handleEvent() {
  auto tmp = tie_.lock();
  if (tied_ && !tmp) {

    return;
  }
  handleEventWithGuard();
}

void Channel::setCloseCallback(Callback cb) { closeCallback_ = std::move(cb); }
void Channel::setReadCallback(Callback cb) { readCallback_ = std::move(cb); }
void Channel::setWriteCallback(Callback cb) { writeCallback_ = std::move(cb); }

bool Channel::enableReading() {
  uint32_t newEvent = events_ | EPOLLIN | EPOLLRDHUP;
  return update_and_rollBack(newEvent);
}

bool Channel::disableReading() {
  uint32_t newEvent = events_ & ~(EPOLLIN | EPOLLRDHUP);
  return update_and_rollBack(newEvent);
}

bool Channel::enableWriting() {
  uint32_t newEvent = events_ | EPOLLOUT;
  return update_and_rollBack(newEvent);
}

bool Channel::disableWriting() {
  uint32_t newEvent = events_ & ~EPOLLOUT;
  return update_and_rollBack(newEvent);
}

bool Channel::disableAll() {
  uint32_t newEvent = 0;
  return update_and_rollBack(newEvent);
}

bool Channel::isWriting() const { return (events_ & EPOLLOUT); }
bool Channel::isReading() const { return (events_ & EPOLLIN); }

void Channel::tie(const std::shared_ptr<void> &p) {
  tie_ = p;
  tied_ = true;
}