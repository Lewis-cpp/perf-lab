#pragma once
#include "net/Channel.h"
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>

class EventLoop;

// ============================================================================
// TcpConnection：单条已建立连接的“包厢专职服务员”
// （归属单个 Sub Reactor 线程，无锁负责该连接的非阻塞收发、64KB 背压与半关闭状态机）
//
// 【所有权与生命周期红线】：
//   - 必须由 std::shared_ptr 创建与持有（继承 enable_shared_from_this）；
//   - 独占拥有 fd_：fd_ 必须先于 channel_ 声明；Poller 销户由 handleClose() 或
//     connectDestroyed() 在所属 loop_ 线程完成，~TcpConnection() 断言已销户后 close(fd_)；
//   - 防回调自杀：start() 中调用 channel_.tie(shared_from_this())，由 Channel::handleEvent()
//     在栈上持临时强引用续命，同时靠 closed_ 拦住同轮后续 handleWrite() 重复操作。
// ============================================================================
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
  using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
  using CloseCallback = std::function<void(const TcpConnectionPtr &)>;

  // 构造时内部焊线（绑定 channel_ 的读写关闭回调），但不主动注册到 Poller
  TcpConnection(EventLoop *loop, int fd);
  // 断言已完成 Poller 销户 (closed_)，最后归还内核 fd_（RAII）
  ~TcpConnection();

  // 禁止拷贝与移动：channel_ 在内核注册 this 指针，且独占管理 fd_
  TcpConnection(const TcpConnection &conn) = delete;
  TcpConnection(TcpConnection &&conn) = delete;
  TcpConnection &operator=(const TcpConnection &conn) = delete;
  TcpConnection &operator=(TcpConnection &&conn) = delete;

  // 在所属 loop_ 线程调用：绑定 channel_.tie(shared_from_this()) 并挂上可读事件开始接客
  [[nodiscard]] bool start();
  void setCloseCallback(CloseCallback cb) { closeCallback_ = std::move(cb); }
  int fd() const { return fd_; }

  EventLoop *getLoop() const { return loop_; }

  // 销毁前最后一道收尾（仅限所属 loop_ 线程调用）：若尚未走过 handleClose()，在此兜底从 Poller 销户
  void connectDestroyed();

private:
  void handleRead();
  void handleWrite();
  void handleClose();
  // 尽量将 output_[sent_..] 刷进内核（处理 EINTR / EAGAIN / 过半裁剪 / 硬错误关闭）
  bool flushOutput();
  // 读写收尾统一结算中心：半关闭排空关闭、动态更新 EPOLLOUT/EPOLLIN、readPaused 强制 MOD 补通知
  void afterIO(bool readPaused);

  static constexpr std::size_t kHighWaterMark =
      64 * 1024; // 64KB 真积压背压高水位

  EventLoop *loop_;
  // 【声明顺序红线】：fd_ 必须排在 channel_ 之前，保证初始化列表先传有效 fd
  const int fd_;
  Channel channel_;
  // 应用层发送缓冲与游标：[0..sent_) 已送进内核，[sent_..size()) 待发剩菜，过半才擦除实现摊还 O(1)
  std::string output_;
  std::size_t sent_ = 0;
  // 对端已发 FIN（recv == 0）：停止继续读，等 output_ 发完再正式关连接
  bool readClosed_ = false;
  // 防重复 handleClose()、防同轮后续 handleWrite() 重复触发，并供析构校验已从 Poller 销户
  bool closed_ = false;
  // 通知上层 TcpServer 发起跨线程移除与销毁接力
  CloseCallback closeCallback_;
};