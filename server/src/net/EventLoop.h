#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <vector>

// 前向声明即可：完整定义留到 EventLoop.cpp；
// 因 p_ 为 std::unique_ptr<Poller>，构造与析构必须在 .cpp 中 out-of-line 定义。
class Poller;
class Channel;

// ============================================================================
// EventLoop：事件驱动心脏 / 火锅店“大堂经理”（对应原 main.cpp 的 while(1) 循环）
//
// 【所有权关系】：
//   - 独占拥有唯一的 Poller (p_)；
//   - 仅借用 activeChannels_ 里的 Channel*，不负责其生命周期。
// ============================================================================
class EventLoop {

private:
  // 独占拥有的底层 I/O 复用管家
  std::unique_ptr<Poller> p_;
  // 优雅停机开关（替代 while(1)）
  std::atomic<bool> quit_ = false;
  // 每轮复用的活跃工牌名单：clear() 保留容量，稳态零堆分配
  std::vector<Channel *> activeChannels_;
  // 本轮 handleEvent() 结束后统一执行的延迟任务（如轮末销毁已关闭的 TcpConnection）
  std::vector<std::function<void()>> pendingFunctors_;

  void doPendingFunctors();

public:
  EventLoop();
  ~EventLoop();

  // 禁止拷贝与移动
  EventLoop(const EventLoop &e) = delete;
  EventLoop(EventLoop &&e) = delete;
  EventLoop &operator=(const EventLoop &e) = delete;
  EventLoop &operator=(EventLoop &&e) = delete;

  // 启动主循环：清空 activeChannels_ -> Poller::poll -> handleEvent() -> doPendingFunctors()
  void loop();

  // 置 quit_ = true，当前轮次结束后退出循环
  void quit();

  // 转发给内部 Poller
  [[nodiscard]] bool updateChannel(Channel *channel);
  void removeChannel(Channel *channel);

  // 投递回调到本轮所有 Channel::handleEvent() 执行完毕后运行
  void queueInLoop(std::function<void()> cb);
};
