#pragma once
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>

class Channel;

// ============================================================================
// Poller：Linux epoll 对讲机管家（全项目唯一允许直接调 epoll_* 的黑盒）
//
// 【所有权红线】：
//   - 独占拥有 epfd_：构造创建，~Poller() 负责 close(epfd_)。
//   - 只借用 Channel*：销毁 Channel 前必须先 removeChannel() 销户，
//     避免 ev.data.ptr 残留悬垂指针。
// ============================================================================
class Poller {
private:
  int epfd_;
  // 接收 epoll_wait 返回的就绪事件数组（满时在 Poller.cpp 中倍增扩容）
  std::vector<struct epoll_event> eventList_;
  // 在册花名册（fd -> Channel*）：用于区分 ADD / MOD，并在 DEL 时防护
  std::unordered_map<int, Channel *> channels_;

public:
  using ChannelList = std::vector<Channel *>;

  // 构造时创建 epfd_ (EPOLL_CLOEXEC) 并 resize(64)，失败直接 abort()
  Poller();
  ~Poller();

  // 禁止拷贝与移动：防止两个 Poller 持有同一 epfd_ 导致 double close
  Poller(const Poller &p) = delete;
  Poller(Poller &&p) = delete;
  Poller &operator=(const Poller &p) = delete;
  Poller &operator=(Poller &&p) = delete;

  // 等待就绪事件并将 Channel* 写入 activeChannels（通过 ev.data.ptr O(1) 找回）
  void poll(ChannelList &activeChannels, int timeout = 1000);

  // 同步 Channel::events() 到内核红黑树（设置 ev.data.ptr = channel）：
  //   - 【EPOLLET 归属契约】：由本函数统一或上 EPOLLET，杜绝 MOD 洗掉 ET；
  //   - 未在册走 ADD（成功后入表），已在册走 MOD；系统调用失败返回 false。
  [[nodiscard]] bool updateChannel(Channel *channel);

  // 从内核红黑树 (EPOLL_CTL_DEL) 与 channels_ 花名册移除
  void removeChannel(Channel *channel);
};