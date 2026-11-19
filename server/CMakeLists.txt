cmake_minimum_required(VERSION 3.16)
project(perf_server CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Reactor 组件
add_library(net STATIC
  src/net/Acceptor.cpp
  src/net/Channel.cpp
  src/net/EventLoop.cpp
  src/net/Poller.cpp
  src/net/TcpConnection.cpp
)
target_include_directories(net PUBLIC src)
target_compile_options(net PRIVATE -Wall -Wextra)

add_executable(server src/main.cpp)
target_compile_options(server PRIVATE -Wall -Wextra)
target_link_libraries(server PRIVATE net)

# net 库冒烟测试（测试代码，不进默认构建）：cmake --build build --target net_smoke && ./build/net_smoke
add_executable(net_smoke EXCLUDE_FROM_ALL tests/test_net_smoke.cpp)
target_link_libraries(net_smoke PRIVATE net)
target_compile_options(net_smoke PRIVATE -Wall -Wextra -fsanitize=address,undefined)
target_link_options(net_smoke PRIVATE -fsanitize=address,undefined)

# Acceptor 冒烟测试：ET 排空 / 无回调自关 / EMFILE 救急与恢复
add_executable(acceptor_smoke EXCLUDE_FROM_ALL tests/test_acceptor_smoke.cpp)
target_link_libraries(acceptor_smoke PRIVATE net)
target_compile_options(acceptor_smoke PRIVATE -Wall -Wextra -fsanitize=address,undefined)
target_link_options(acceptor_smoke PRIVATE -fsanitize=address,undefined)
