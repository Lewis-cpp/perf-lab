// ============================================================================
// W1 阶段二：基于 epoll (LT 水平触发) + 非阻塞 I/O 的高并发 Echo Server
//
// 核心架构类比（火锅店模型）：
//   - listen_fd  : 站在店门口的“迎宾员”（只负责招揽客人，绝不进包厢端茶倒水）
//   - conn_fd    : accept4() 孵化出的“包厢专职服务员”（专门服务某一桌客人）
//   - ep_fd      : 唯一“大堂经理”（主线程）手里的对讲机，哪个包厢亮灯就去处理一下，
//                  必须做到“秒进秒出”，绝不能在任何一个包厢里阻塞睡觉！
//
// 本文件已攻克的六大生产级难关：
//   1. accept4(SOCK_NONBLOCK) 解决新连接不继承 listen_fd 非阻塞属性的问题
//   2. 预留备胎 fd (idle_fd) 解决进程 fd 耗尽 (EMFILE) 时 LT 模式死循环刷日志问题
//   3. 读写事件分离 (EPOLLIN / EPOLLOUT) + 应用层发送缓冲区，防止慢客户端卡死全服
//   4. 正确区分 recv/send 返回值（>0, ==0, -1 中的软错误 EAGAIN/EINTR 与硬错误）
//   5. read_closed 半关闭标记，解决客户端 shutdown(SHUT_WR) 后服务端丢弃未发完回显问题
//   6. 64KB 高水位背压 (Backpressure) + 4KB 游标定期裁剪 + events 影子状态防冗余 epoll_ctl
// ============================================================================

#include <arpa/inet.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>

// 每个客户端连接专属的“用户态账本”（对应 W2 架构中的 Buffer + Channel 雏形）
struct Connection {
  // 应用层发送缓冲区（包厢门口的“备菜架”）：
  // 当内核发送缓冲区被慢客户端塞满（send 返回 EAGAIN）时，没发完的“剩菜”暂存在这里
  std::string output;

  // 游标（已发送字节偏移量，类似 Muduo Buffer 的 readerIndex）：
  // 记录 output 前面有多少字节已经成功塞进内核了，避免每次 send 都 erase(0, n) 造成 O(N) 内存搬移
  // 内存布局：[ 0 ... sent (已发旧数据) | sent ... output.size() (待发剩菜) ]
  std::size_t sent = 0;

  // 对端半关闭标记（客人是否已经合上菜单说“不再点新菜了”）：
  // 当 recv() 返回 0（收到客户端 FIN 包，进入 CLOSE_WAIT）时置为 true。
  // 此时读通道已关，但写通道依然畅通，必须等 output 里的剩菜全发完才能真正 close(fd)！
  bool read_closed = false;

  // 内核 epoll 事件的“用户态影子状态”（对应 Muduo Channel::events_）：
  // 记住当前 fd 在内核红黑树里已经注册了什么事件。
  // 只有当期望的新事件与它不同时才调用 epoll_ctl(MOD)，消灭 99.9% 的冗余系统调用！
  uint32_t events = EPOLLIN;
};

int main() {
  int optval = 1;

  // 【防线一：备胎 fd (idle_fd) 占位法】
  // 预先打开 /dev/null 占住 1 个文件描述符坑位，揣在怀里不用。
  // 当高并发把进程 fd 配额 (ulimit -n) 耗尽，accept4 报错 EMFILE 时，
  // 靠它腾出 1 个临时空位把全连接队列队头的客人领出来并礼貌劝退，防止 LT 模式 100% CPU 忙循环！
  int idle_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);

  // 1. 创建监听套接字（迎宾员上岗）
  // SOCK_NONBLOCK：将 listen_fd 设为非阻塞（注：此属性放在 VFS struct file->f_flags 上，不会遗传给 accept 出的新连接！）
  int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (listen_fd < 0) {
    std::perror("creat socket failed.");
    return -1;
  }

  // 2. 开启 SO_REUSEADDR（必须在 bind 之前设置！）
  // 允许服务器重启时忽略内核 ehash 表中还处于 TIME_WAIT 状态（等待 2MSL）的旧连接，立刻绑定端口成功，
  // 彻底告别重启时的 "Address already in use" 报错。
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) <
      0) {
    std::perror("set reuseaddr failed.");
    return -1;
  }

  // 3. 绑定本地 IP 与端口
  // sockaddr_in 是 IPv4 专用的 16 字节地址结构，传给 bind 时强转为通用基类 sockaddr*
  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(8888);              // Host to Network Short：主机小端序转网络大端序（16位端口）
  addr.sin_addr.s_addr = htonl(INADDR_ANY); // INADDR_ANY (0.0.0.0)：监听本机所有网卡（lo、内网、公网全收）

  if (bind(listen_fd, (const sockaddr *)&addr, sizeof(addr)) < 0) {
    std::perror("bind failed.");
    return -1;
  }

  // 4. 切换为被动监听状态 (LISTEN)，内核为其开辟【半连接队列】与【全连接队列 (Accept Queue)】
  // 注意：listen() 瞬间返回不阻塞！TCP 三次握手由内核迎宾员在后台独立完成，握完手的连接排进全连接队列。
  if (listen(listen_fd, 128) < 0) {
    std::perror("listen failed.");
    return -1;
  }

  // 5. 创建 epoll 实例（给大堂经理配发对讲机，内核底层创建红黑树 rbr + 就绪链表 rdllist）
  int ep_fd = epoll_create1(0);
  if (ep_fd == -1) {
    std::perror("epoll create failed.");
    return -1;
  }

  // 将迎宾员 listen_fd 挂到 epoll 红黑树上，关心 EPOLLIN（全连接队列有新客人来）
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = listen_fd; // 记录是谁触发了事件
  if (epoll_ctl(ep_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
    std::perror("epoll_ctl: ADD failed");
    return -1;
  }

  struct epoll_event events[64];
  std::unordered_map<int, Connection> connections;

  // ==================== 6. 核心事件循环 (Event Loop) ====================
  while (1) {
    // 阻塞等待对讲机响起（最多等 1000ms），内核把就绪的 fd 列表拷贝到 events 数组中
    int n = epoll_wait(ep_fd, events, sizeof(events) / sizeof(events[0]), 1000);
    if (n < 0) {
      // EINTR 是“假错误”：休眠等待时被系统信号吵醒打断，连接和 epoll 完好无损，直接继续循环
      if (errno == EINTR)
        continue;
      std::perror("epoll_wait failed.");
      return -1;
    }

    for (int i = 0; i < n; ++i) {
      int fd = events[i].data.fd;

      // ---------------- 分支 A：迎宾员 (listen_fd) 亮灯，有新客人握完手了 ----------------
      if (fd == listen_fd) {
        // 【关键陷阱防御】accept 创建的新 struct file 默认不会继承 listen_fd 的非阻塞属性！
        // 必须用 Linux 专用的 accept4(..., SOCK_NONBLOCK) 一步到位把新连接焊死为非阻塞，
        // 否则后续一旦遇到慢客户端，阻塞的 send() 会让唯一的主线程直接休眠，卡死全服所有人！
        fd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK);
        if (fd == -1) {
          if (errno == EMFILE || errno == ENFILE) {
            // 【备胎救急四步曲】进程的 fd 号码牌发完了！
            // 新连接已经躺在了内核全连接队列里，但因为拿不到新 fd 取不出来；
            // 若不取走，LT 模式下 epoll_wait 会 0ms 无限返回可读，导致 CPU 100% 狂刷日志！
            std::perror(
                "accept failed(EMFILE): fd exhausted, dropping connection");
            close(idle_fd);                                    // 1. 释放备胎，腾出 1 个 fd 空位
            idle_fd = accept(listen_fd, nullptr, nullptr);     // 2. 把卡在全连接队列头的客人领进门
            close(idle_fd);                                    // 3. 立刻关掉劝退，同时再次腾出空位
            idle_fd = open("/dev/null", O_RDONLY | O_CLOEXEC); // 4. 重新把备胎揣回怀里
          } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            std::perror("accept failed.");
          continue;
        }

        // 为新客人建一本专属账本，初始只关心“可读灯 (EPOLLIN)”
        // 切记：绝不能一上来就挂 EPOLLOUT！因为内核发送缓冲区 99.9% 时间是空的，常挂 EPOLLOUT 会导致 CPU 100% 空转！
        connections[fd] = Connection();
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        epoll_ctl(ep_fd, EPOLL_CTL_ADD, fd, &ev);

      // ---------------- 分支 B：包厢服务员 (conn_fd) 亮灯，处理客户端读写 ----------------
      } else {
        uint32_t revents = events[i].events; // 本次实际亮起的指示灯位图 (Bitmask)
        auto &conn = connections[fd];

        // ==================== 1. 处理可读事件 (EPOLLIN) ====================
        // 注意：EPOLLIN 和 EPOLLOUT 必须用两个独立的 if（不能用 else if），因为两盏灯可能同时亮！
        if (revents & EPOLLIN) {
          char buff[1024 * 64];
          ssize_t len = recv(fd, buff, sizeof(buff), 0);
          if (len > 0) {
            // 1a. 成功从内核接收缓冲区搬运了 len 字节，追加到应用层待发队列末尾
            conn.output.append(buff, len);

          } else if (len == 0) {
            // 1b. 返回 0 的唯一含义：对端调用了 close() 或 shutdown(SHUT_WR)，发来了 FIN 包 (EOF)！
            // 【防截断关键】此时只代表客户端“不再发新数据了”，不代表它不收回显了！
            // 绝不能当场 close(fd) 掀桌子，否则 conn.output 里还没发完的积压回显会被全部丢弃！
            // 这里只打上“读通道已关闭”标记，继续往下走去把剩菜发完。
            conn.read_closed = true;
          } else {
            // 1c. 返回 -1：掏出 errno 诊断书区分“假错误”与“真错误”
            // EAGAIN / EWOULDBLOCK：非阻塞模式下当前没数据可读；EINTR：被信号打断 —— 连接好好的，绝不能关！
            // 只有像 ECONNRESET（对方掀桌子发来 RST）这样的硬错误，才真正关闭连接回收资源。
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
              std::perror("recv failed.");
              close(fd);
              connections.erase(fd);
              continue;
            }
          }
        }

        // ==================== 2. 处理发送与可写事件 (EPOLLOUT) ====================
        // 只要账本里还有未发送的剩菜（无论是刚才 EPOLLIN 新收到的，还是之前积压、被 EPOLLOUT 唤醒的），
        // 都尝试往内核发送缓冲区塞一次！
        if (conn.sent < conn.output.size()) {
          // MSG_NOSIGNAL：如果对方已经断开连接，防止内核产生 SIGPIPE 信号直接杀死整个服务端进程！
          ssize_t nwritten = send(fd, conn.output.data() + conn.sent,
                                  conn.output.size() - conn.sent, MSG_NOSIGNAL);
          if (nwritten > 0) {
            conn.sent += nwritten; // 成功塞进内核 nwritten 字节，游标 O(1) 往前推

            // 【防僵尸前缀】如果前面已经发走的废弃字节攒够了 4KB，且后面还有剩菜没发完，
            // 就顺手裁掉前半段，防止客户端长期“发快收慢”导致已发送的历史数据无限堆积。
            if (conn.sent >= 4096 && conn.sent < conn.output.size()) {
              conn.output.erase(0, conn.sent);
              conn.sent = 0;
            }

          } else if (nwritten == -1 && errno != EAGAIN &&
                     errno != EWOULDBLOCK && errno != EINTR) {
            // 如果返回 -1 且是 EAGAIN：说明遇到了慢客户端，内核发送缓冲区已满！
            // 我们绝不在原地 while 死等，而是什么都不做，留给第 3 步去挂上 EPOLLOUT。
            // 只有遇到 EPIPE / ECONNRESET 等真错误，才关闭连接！
            std::perror("send failed.");
            close(fd);
            connections.erase(fd);
            continue;
          }
        }

        // ==================== 3. 动态调整事件、流控背压与收尾 ====================
        std::size_t unsent = conn.output.size() - conn.sent; // 当前真正积压未发的字节数
        constexpr std::size_t kHighWaterMark = 64 * 1024;    // 64KB 高水位线

        // 收尾检查：剩菜已经 100% 端上桌了，且客人之前就说不再点菜了 (read_closed) ──► 功德圆满，正式关连接！
        // （注：close(fd) 会使内核 struct file 引用计数归零，epoll 会自动将其从红黑树移除）
        if (unsent == 0 && conn.read_closed) {
          close(fd);
          connections.erase(fd);
          continue;
        }

        // 如果所有积压数据都发完了，清空账本并重置游标，彻底释放内存
        if (unsent == 0 && !conn.output.empty()) {
          conn.output.clear();
          conn.sent = 0;
        }

        // 计算接下来我们【期望】对讲机监听哪几盏灯 (new_event)
        uint32_t new_event = 0;
        if (unsent > 0) {
          // 还有剩菜没发完，必须挂上 EPOLLOUT，等慢客户端收走数据、内核缓冲区腾出空位时再叫我。
          // 什么时候必须摘掉 EPOLLIN（只留 EPOLLOUT）？
          //   情况 1 (conn.read_closed)：对端已发来 FIN，在 LT 模式下 EOF 会使 EPOLLIN 永远常亮，不摘掉会 100% CPU 死循环！
          //   情况 2 (unsent >= kHighWaterMark)：【高水位背压 Backpressure】如果“只发不收”的慢客户端已积压满 64KB，
          //          我们主动暂停调用 recv()！服务端内核接收缓冲区很快被塞满，会自动通过 TCP 滑动窗口 (ZeroWindow)
          //          把客户端自己的 send() 卡死，从而保护服务端用户态内存绝不被撑爆 (OOM)！
          new_event = (conn.read_closed || unsent >= kHighWaterMark)
                          ? EPOLLOUT
                          : (EPOLLOUT | EPOLLIN);
        } else {
          // 没有积压数据，安安静静只监听可读事件 EPOLLIN
          new_event = EPOLLIN;
        }

        // 【消除冗余系统调用】先在用户态对比影子状态 conn.events（耗时 < 1ns）：
        // 只有当期望的事件组合真正发生变化时，才陷入内核调 epoll_ctl(MOD) 修改红黑树！
        if (new_event != conn.events) {
          conn.events = new_event;
          ev.events = new_event;
          ev.data.fd = fd;
          epoll_ctl(ep_fd, EPOLL_CTL_MOD, fd, &ev);
        }
      }
    }
  }
}
