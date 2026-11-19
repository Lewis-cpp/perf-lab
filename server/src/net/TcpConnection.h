#pragma once
#include "net/Channel.h"
#include <cstddef>
#include <functional>
#include <string>
#include <utility>

class EventLoop;

// ============================================================================
// TcpConnection：单条已建立连接的“包厢专职服务员”
// （接管原 main.cpp 的 struct Connection 与分支 B 全部收发、背压、半关闭状态机）
//
// 【所有权与生命周期红线】：
//   - 独占拥有 conn_fd_：~TcpConnection() 负责 ::close(fd_)，且 fd_ 必须排在 channel_ 之前声明。
//   - 延迟销毁契约：若在 Channel::handleEvent() 回调中触发 handleClose()，绝不能当场 delete
//     this（否则返回 handleEvent() 后读 revents_ 会引发 use-after-free），需由上层通过
//     EventLoop::queueInLoop() 移到轮末清理，同时靠 closed_ 拦住同轮后续 handleWrite() 鞭尸。
// ============================================================================
class TcpConnection {
public:
  using CloseCallback = std::function<void()>;

  // 构造时内部焊线（绑定 channel_ 的读写关闭回调），但不主动注册到 Poller
  TcpConnection(EventLoop *loop, int fd);
  // 若尚未触发 handleClose()，先从 Poller 销户，再归还 fd_（RAII）
  ~TcpConnection();

  // 禁止拷贝与移动：channel_ 在内核注册 this 指针，且独占管理 fd_
  TcpConnection(const TcpConnection &conn) = delete;
  TcpConnection(TcpConnection &&conn) = delete;
  TcpConnection &operator=(const TcpConnection &conn) = delete;
  TcpConnection &operator=(TcpConnection &&conn) = delete;

  // 等上层 setCloseCallback() 绑好后再调：挂上可读事件正式接客，ADD 失败返回 false
  [[nodiscard]] bool start();
  void setCloseCallback(CloseCallback cb) { closeCallback_ = std::move(cb); }
  int fd() const { return fd_; }

private:
  void handleRead();
  void handleWrite();
  void handleClose();
  // 尽量将 output_[sent_..] 刷进内核（处理 EINTR / EAGAIN / 过半裁剪 / 硬错误关闭）
  bool flushOutput();
  // 读写收尾统一结算中心：半关闭排空关闭、动态更新 EPOLLOUT/EPOLLIN、readPaused 强制 MOD 补通知
  void afterIO(bool readPaused);

  static constexpr std::size_t kHighWaterMark = 64 * 1024; // 64KB 真积压背压高水位

  EventLoop *loop_;
  // 【声明顺序红线】：fd_ 必须排在 channel_ 之前，保证初始化列表先传有效 fd
  const int fd_;
  Channel channel_;
  // 应用层发送缓冲与游标：[0..sent_) 已送进内核，[sent_..size()) 待发剩菜，过半才擦除实现摊还 O(1)
  std::string output_;
  std::size_t sent_ = 0;
  // 对端已发 FIN（recv == 0）：停止继续读，等 output_ 发完再正式关连接
  bool readClosed_ = false;
  // 防重复 handleClose() 与防同轮后续 handleWrite() 鞭尸
  bool closed_ = false;
  // 通知上层老板（main / Server）将本连接移出活跃账本
  CloseCallback closeCallback_;
};