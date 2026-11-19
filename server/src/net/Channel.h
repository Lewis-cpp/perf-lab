#pragma once
#include <cstdint>
#include <functional>

// 前向声明即可：头文件仅持指针，实际转发留在 Channel.cpp。
class EventLoop;

// ============================================================================
// Channel：单个 fd 的“智能呼叫工牌”（对应原 main.cpp 的 events 状态与分支分发）
//
// 职责：
//   - 将 fd、关注事件掩码 (events_)、内核就绪位图 (revents_) 及回调打包；
//   - 配合 ev.data.ptr = this，让 epoll_wait 返回后 O(1) 直达对象。
//
// 【所有权红线】：
//   - Channel 绝不拥有 fd_ 的生命周期（析构绝不 close(fd_)）；
//   - fd_ 由 Acceptor / TcpConnection 管理，Channel 只借用 EventLoop*。
// ============================================================================
class Channel {
public:
  using Callback = std::function<void()>;

  Channel(EventLoop *loop, int fd) : loop_(loop), fd_(fd) {}

  // 禁止拷贝与移动：注册后内核 epitem.event.data.ptr 存死 this 地址，不可挪窝。
  Channel(const Channel &c) = delete;
  Channel(Channel &&c) = delete;
  Channel &operator=(const Channel &c) = delete;
  Channel &operator=(Channel &&c) = delete;

  // -------------------- 1. 给 EventLoop::loop() 调用 --------------------
  void handleEvent();

  // -------------------- 2. 给上层主人 (Acceptor / TcpConnection) 调用 --------
  void setReadCallback(Callback cb);
  void setWriteCallback(Callback cb);
  void setCloseCallback(Callback cb);

  // 订阅事件开关（仅改 events_ 的纯读写位；失败自动回滚并返回 false）：
  //   - enableReading / disableReading：管 EPOLLIN | EPOLLRDHUP；
  //     高水位背压或半关闭 (read_closed) 且有剩菜时需单独 disableReading() 保留 EPOLLOUT。
  //   - enableWriting / disableWriting：管 EPOLLOUT。
  //   - disableAll：清零关注事件。
  [[nodiscard]] bool enableReading();
  [[nodiscard]] bool disableReading();
  [[nodiscard]] bool enableWriting();
  [[nodiscard]] bool disableWriting();
  [[nodiscard]] bool disableAll();
  bool isWriting() const;
  bool isReading() const; // 供上层判断当前是否在监听读（如 read_paused 强制 MOD 补通知前检查）

  // -------------------- 3. 给 Poller 调用的读写器 --------------------
  int fd() const { return fd_; }
  uint32_t events() const { return events_; }
  void set_revents(uint32_t revt) { revents_ = revt; }

  // -------------------- 4. 同步 / 摘除红黑树 --------------------
  // update()：将 events_ 同步到 Poller（ADD 或 MOD）；
  //           ET 下若发生 read_paused 竞态需掩码不变也强制 MOD，直接调它即可。
  // remove()：在关闭 fd 前从红黑树与花名册销户（EPOLL_CTL_DEL）。
  [[nodiscard]] bool update();
  void remove();

private:
  EventLoop *loop_;
  const int fd_;
  // 只维护纯读写语义位（EPOLLIN/EPOLLRDHUP/EPOLLOUT），兼作用户态影子状态；
  // 【EPOLLET 契约】：EPOLLET 由 Poller::updateChannel() 统一附加，避免位运算误洗回 LT。
  uint32_t events_ = 0;
  // 本轮 epoll_wait 返回的实际就绪位图
  uint32_t revents_ = 0;
  Callback readCallback_, writeCallback_, closeCallback_;

  // 掩码未变时自动去重返回 true；调用 update() 失败则回滚为 old，严守影子状态一致。
  [[nodiscard]] bool update_and_rollBack(uint32_t newevent) {
    if (newevent == events_)
      return true;
    uint32_t old = events_;
    events_ = newevent;
    if (update())
      return true;
    else {
      events_ = old;
      return false;
    }
  }
};