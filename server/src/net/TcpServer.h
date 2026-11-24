#pragma once

#include "net/TcpConnection.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>

class EventLoop;
class Acceptor;
class EventLoopThreadPool;

// ============================================================================
// TcpServer：主从多线程 Reactor 总控（火锅店“大堂总店长”）
//
// 核心并发流转：
//   1) 建连分发 (MainLoop -> SubLoop)：Acceptor 在主线程 accept 新 fd -> getNextLoop()
//      轮询挑出 ioLoop -> 登记进 connections_ -> ioLoop->runInLoop() 跨线程挂树接客；
//   2) 三跳关闭 (SubLoop -> MainLoop -> SubLoop)：连接在 ioLoop 触发 handleClose()
//      -> removeConnection() 投递回主线程 -> removeConnectionInLoop() 无锁擦除 connections_
//      -> queueInLoop() 投回原 ioLoop 执行 connectDestroyed() 并释放最后一份 shared_ptr。
//
// 【所有权与线程边界红线】：
//   - connections_ 完全无锁，严格只在主循环线程 (loop_) 内读写；
//   - 因持有前向声明类的 unique_ptr，~TcpServer() 必须在 .cpp 中定义。
// ============================================================================
class TcpServer {
public:
  TcpServer(EventLoop *loop, std::uint16_t port);
  ~TcpServer();
  TcpServer(const TcpServer &a) = delete;
  TcpServer(TcpServer &&a) = delete;
  TcpServer &operator=(const TcpServer &a) = delete;
  TcpServer &operator=(TcpServer &&a) = delete;

  // 设置 Sub Reactor 线程数（必须在 start() 前调用；0 为单线程模式）
  void setThreadNum(size_t n);
  // 先启动 IO 线程池，再开启 Acceptor 监听
  void start();

private:
  // 主线程回调：为新连接分配 ioLoop、登记账本并跨线程启动
  void newConnection(int fd);
  // 关闭第 1 跳（可在任意 SubLoop 调用）：按值捕获 conn 转投主线程
  void removeConnection(const TcpConnection::TcpConnectionPtr &conn);
  // 关闭第 2 跳（主线程执行）：从 connections_ 擦除后排回所属 SubLoop 执行 connectDestroyed()
  void removeConnectionInLoop(const TcpConnection::TcpConnectionPtr &conn);

  EventLoop *loop_; // 借用的 Main Reactor（不负责销毁）
  std::unique_ptr<Acceptor> acceptor_;
  std::unique_ptr<EventLoopThreadPool> threadPool_;
  bool started_ = false;
  // 活跃连接总账本（fd -> TcpConnectionPtr）：fd 直至 ~TcpConnection() 才 close，在册期间唯一不复用
  std::unordered_map<int, TcpConnection::TcpConnectionPtr> connections_;
};