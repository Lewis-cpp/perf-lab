cmake_minimum_required(VERSION 3.16)
project(perf_server CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(Threads REQUIRED)

# 主从 Reactor 网络核心库
add_library(net STATIC
  src/net/Acceptor.cpp
  src/net/Channel.cpp
  src/net/EventLoop.cpp
  src/net/EventLoopThread.cpp
  src/net/EventLoopThreadPool.cpp
  src/net/Poller.cpp
  src/net/TcpConnection.cpp
  src/net/TcpServer.cpp
)
target_include_directories(net PUBLIC src)
target_compile_options(net PRIVATE -Wall -Wextra)
target_link_libraries(net PUBLIC Threads::Threads)

# 默认主程序（主从多线程 Echo Server）
add_executable(server src/main.cpp)
target_compile_options(server PRIVATE -Wall -Wextra)
target_link_libraries(server PRIVATE net)

# -------------------- 专项冒烟测试（EXCLUDE_FROM_ALL，按需构建） --------------------

# 1. EventLoop / Channel / Poller / Channel::tie 冒烟测试（ASan + UBSan）
add_executable(net_smoke EXCLUDE_FROM_ALL tests/test_net_smoke.cpp)
target_link_libraries(net_smoke PRIVATE net)
target_compile_options(net_smoke PRIVATE -Wall -Wextra -fsanitize=address,undefined)
target_link_options(net_smoke PRIVATE -fsanitize=address,undefined)

# 2. Acceptor 冒烟测试：ET 排空 / 无回调自关 / EMFILE 备胎救急与恢复（ASan + UBSan）
add_executable(acceptor_smoke EXCLUDE_FROM_ALL tests/test_acceptor_smoke.cpp)
target_link_libraries(acceptor_smoke PRIVATE net)
target_compile_options(acceptor_smoke PRIVATE -Wall -Wextra -fsanitize=address,undefined)
target_link_options(acceptor_smoke PRIVATE -fsanitize=address,undefined)

# 3. EventLoop 跨线程唤醒：eventfd / runInLoop / 多生产者投递（TSan 独立编译，不混链 libnet.a）
add_executable(wakeup_smoke EXCLUDE_FROM_ALL tests/test_wakeup.cpp
  src/net/Channel.cpp src/net/EventLoop.cpp src/net/Poller.cpp)
target_include_directories(wakeup_smoke PRIVATE src)
target_compile_options(wakeup_smoke PRIVATE -Wall -Wextra -fsanitize=thread)
target_link_options(wakeup_smoke PRIVATE -fsanitize=thread)

# 4. EventLoopThread / EventLoopThreadPool 生命周期与轮询分发冒烟测试（TSan）
add_executable(threadpool_smoke EXCLUDE_FROM_ALL tests/test_threadpool.cpp
  src/net/Channel.cpp src/net/EventLoop.cpp src/net/EventLoopThread.cpp
  src/net/EventLoopThreadPool.cpp src/net/Poller.cpp)
target_include_directories(threadpool_smoke PRIVATE src)
target_compile_options(threadpool_smoke PRIVATE -Wall -Wextra -fsanitize=thread)
target_link_options(threadpool_smoke PRIVATE -fsanitize=thread)

# 5. TcpServer 主从多线程端到端并发与三跳关闭验收测试（TSan）
add_executable(tcpserver_smoke EXCLUDE_FROM_ALL tests/test_tcpserver.cpp
  src/net/Acceptor.cpp
  src/net/Channel.cpp
  src/net/EventLoop.cpp
  src/net/EventLoopThread.cpp
  src/net/EventLoopThreadPool.cpp
  src/net/Poller.cpp
  src/net/TcpConnection.cpp
  src/net/TcpServer.cpp
)
target_include_directories(tcpserver_smoke PRIVATE src)
target_compile_options(tcpserver_smoke PRIVATE -Wall -Wextra -fsanitize=thread)
target_link_options(tcpserver_smoke PRIVATE -fsanitize=thread)
target_link_libraries(tcpserver_smoke PRIVATE Threads::Threads)

# 6. ASan + UBSan 版 server（供 tests/test_echo.py 1000 并发黑盒压测使用，不污染全局标志以免与 TSan 冲突）
add_executable(server_asan EXCLUDE_FROM_ALL
  src/main.cpp
  src/net/Acceptor.cpp
  src/net/Channel.cpp
  src/net/EventLoop.cpp
  src/net/EventLoopThread.cpp
  src/net/EventLoopThreadPool.cpp
  src/net/Poller.cpp
  src/net/TcpConnection.cpp
  src/net/TcpServer.cpp
)
target_include_directories(server_asan PRIVATE src)
target_compile_options(server_asan PRIVATE -g -Wall -Wextra -fsanitize=address,undefined)
target_link_options(server_asan PRIVATE -fsanitize=address,undefined)
target_link_libraries(server_asan PRIVATE Threads::Threads)
