#include "net/Poller.h"
#include "net/Channel.h"
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>

#include <cerrno>
#include <sys/epoll.h>
#include <unistd.h>

Poller::Poller() {
  epfd_ = epoll_create1(EPOLL_CLOEXEC);
  if (epfd_ < 0) {
    std::perror("epoll create failed.");
    abort();
  }
  // 必须用 resize() 分配实际元素个数，确保传给 epoll_wait 的 maxevents > 0
  eventList_.resize(64);
}

Poller::~Poller() { close(epfd_); }

void Poller::poll(ChannelList &activeChannels, int timeout) {
  int n = epoll_wait(epfd_, eventList_.data(), eventList_.size(), timeout);

  if (n < 0) {
    // EINTR 是信号中断假错误，直接返回让外层 loop() 进入下一轮；
    // 其余错误 (EBADF/EINVAL/EFAULT) 均无法自愈，立即 abort 防止 100% CPU 忙循环
    if (errno == EINTR)
      return;
    std::perror("epoll_wait failed.");
    abort();
  }

  for (int i = 0; i < n; ++i) {
    auto &ev = eventList_[i];
    Channel *channel = static_cast<Channel *>(ev.data.ptr);
    channel->set_revents(ev.events);
    activeChannels.push_back(channel);
  }

  // 本轮事件数打满数组时翻倍扩容，便于高并发下一次取光就绪事件
  if (static_cast<size_t>(n) == eventList_.size())
    eventList_.resize(eventList_.size() * 2);
}

bool Poller::updateChannel(Channel *channel) {
  // 零初始化防脏位；ev.data 是 union，填了 ptr 绝不能再碰 fd
  struct epoll_event ev{};
  ev.events = channel->events() | EPOLLET;
  ev.data.ptr = channel;
  int fd = channel->fd();

  auto it = channels_.find(fd);
  if (it == channels_.end()) {
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
      std::perror("epoll add failed.");
      return false; // ADD 失败不写入花名册，保持状态同步
    }
    channels_[fd] = channel;
  } else {
    // 防生命周期/fd 复用 Bug：同 fd 下必须是同一个 Channel 对象
    assert(it->second == channel && "channles doesn't match the channel");
    if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
      std::perror("epoll mod failed.");
      return false;
    }
  }
  return true;
}

void Poller::removeChannel(Channel *channel) {
  int fd = channel->fd();
  auto it = channels_.find(fd);
  if (it == channels_.end()) {
    std::fprintf(stderr, "couldn't find the channel,can not remove it.\n");
    return;
  }

  assert(it->second == channel && "channles doesn't match the channel");
  if (epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) < 0)
    std::perror("remove failed.");

  // 必须擦除，否则内核后续把同一 fd 整数复用给新连接时会误命中旧记录
  channels_.erase(it);
}