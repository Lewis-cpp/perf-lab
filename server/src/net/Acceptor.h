#pragma once
#include "net/Channel.h"
#include <cstdint>
#include <functional>
#include <utility>

class EventLoop;

// ============================================================================
// Acceptor：Main Reactor 专职“门口迎宾员”
//
// 职责边界：
//   - 负责创建非阻塞监听套接字 (SO_REUSEADDR + bind + listen)，并在 ET 模式下
//     循环 accept4() 掏空全连接队列（含 EINTR/ECONNABORTED 重试与 EMFILE 备胎排空）。
//   - 拿到新连接 conn_fd 后立即通过 NewConnectionCall 回调转交 TcpServer 分发，
//     绝不插手后续连接生命周期与读写。
//
// 【所有权红线】：
//   - 独占拥有 listenFd_ 与 idleFd_：~Acceptor() 必须先 channel_.remove() 销户，
//     再关闭 listenFd_ 与 idleFd_。
//   - 只借用所属 Main EventLoop*。
// ============================================================================
class Acceptor {
public:
  using NewConnectionCall = std::function<void(int)>;

  Acceptor(EventLoop *loop, uint16_t port);
  ~Acceptor();

  // 禁止拷贝与移动：内部 channel_ 与内核红黑树绑定，且独占管理 listenFd_/idleFd_
  Acceptor(const Acceptor &a) = delete;
  Acceptor(Acceptor &&a) = delete;
  Acceptor &operator=(const Acceptor &a) = delete;
  Acceptor &operator=(Acceptor &&a) = delete;

  void setNewConnectionCallback(NewConnectionCall cb) {
    newConnectionCallback_ = std::move(cb);
  }

  // 与构造分离：让上层先绑定好 NewConnectionCallback，再 ::listen() 并挂树开张
  void listen();

private:
  void handleRead();

  EventLoop *loop_;
  // 【声明顺序红线】：listenFd_ 必须排在 channel_ 之前！
  // C++ 成员按类内声明顺序在初始化列表中构造，确保 channel_(loop, listenFd_) 拿到已创建好的 fd
  int listenFd_;
  int idleFd_;
  Channel channel_;
  NewConnectionCall newConnectionCallback_;
};