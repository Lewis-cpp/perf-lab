// ============================================================================
// W1 阶段二（升级版）：基于 epoll (ET 边缘触发) + 非阻塞 I/O 的高并发 Echo Server
//
// 核心架构类比（火锅店模型）：
//   - listen_fd  : 站在店门口的“迎宾员”（只负责招揽客人，绝不进包厢端茶倒水）
//   - conn_fd    : accept4() 孵化出的“包厢专职服务员”（专门服务某一桌客人）
//   - ep_fd      : 唯一“大堂经理”（主线程）手里的对讲机，哪个包厢亮灯就去处理一下，
//                  必须做到“秒进秒出”，绝不能在任何一个包厢里阻塞睡觉！
//
// LT vs ET 关键转折（内核 fs/eventpoll.c:ep_send_events）：
//   - LT 模式下，交付事件后内核会把未处理完的 epitem 重新放回 rdllist；
//   - ET（EPOLLET）模式下，内核摘链交付后不再放回，只会在新事件到来时再通知一次！
//     因此迎宾员与服务员都必须做到“要么彻底掏空到 EAGAIN，要么明确记账补通知”。
//
// 本文件已攻克的六大生产级难关：
//   1. listen_fd 与 conn_fd 全程 EPOLLET：accept4 / recv / send 均循环到 EAGAIN，
//      且正确处理 EINTR（不可提前 break）与 EMFILE 备胎队列排空（凭 accept 返回值判定）
//   2. accept4(SOCK_NONBLOCK | SOCK_CLOEXEC) 一步焊死新连接非阻塞，不依赖继承
//   3. 读写事件分离 (EPOLLIN / EPOLLOUT) + 用户态发送缓冲 + MSG_NOSIGNAL 防 SIGPIPE
//   4. read_closed 半关闭标记 + EPOLLRDHUP：对端 shutdown(SHUT_WR) 时发完剩菜再关连接
//   5. 边读边发 flush_output + 64KB 真积压背压 + 已发过半游标裁剪（摊还 O(1)）
//   6. read_paused 补通知机制 + 全掩码保 ET：消灭两次 flush 间的微秒级竞态死锁，
//      兼顾单线程 Reactor 调度公平性，并在 epoll_ctl(MOD) 成功后才更新影子状态
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
  // 记录 output 前面有多少字节已经成功塞进内核了，避免每次 send 都 erase(0, n)
  // 造成 O(N) 内存搬移；只有当已发废数据超过总长一半时才一次性裁剪，摊还 O(1)
  // 内存布局：[ 0 ... sent (已发旧数据) | sent ... output.size() (待发剩菜) ]
  std::size_t sent = 0;

  // 对端半关闭标记（客人是否已经合上菜单说“不再点新菜了”）：
  // 当 recv() 返回 0（收到客户端 FIN 包，进入 CLOSE_WAIT）时置为 true。
  // 此时读通道已关，但写通道依然畅通，必须等 output 里的剩菜全发完才能真正 close(fd)！
  bool read_closed = false;

  // 内核 epoll 事件的“用户态影子状态”（对应 Muduo Channel::events_）：
  // 记住当前 fd 在内核红黑树里已经注册了什么事件；初值必须与 EPOLL_CTL_ADD 时严格一致！
  // 只有当期望的新事件不同（或因高水位暂停读欠了一次通知）时才调用 epoll_ctl(MOD)，
  // 消灭 99.9% 的冗余系统调用。
  uint32_t events = EPOLLIN | EPOLLRDHUP | EPOLLET;
};

int main() {
  int optval = 1;

  // 【防线一：备胎 fd (idle_fd) 占位法】
  // 预先打开 /dev/null 占住 1 个文件描述符坑位，揣在怀里不用。
  // 当高并发把进程 fd 配额 (ulimit -n) 耗尽、accept4 报错 EMFILE 时，
  // 靠它腾出 1 个临时空位把全连接队列队头的客人领出来并立刻 close() 劝退，
  // 防止已握手的连接无限积压在全连接队列里！
  int idle_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);

  // 1. 创建监听套接字（迎宾员上岗）
  // SOCK_NONBLOCK：将 listen_fd 设为非阻塞（注：此属性放在 VFS struct file->f_flags 上，
  // 不会遗传给 accept 出的新连接！）
  int listen_fd =
      socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (listen_fd < 0) {
    std::perror("creat socket failed.");
    return -1;
  }

  // 2. 开启 SO_REUSEADDR（必须在 bind 之前设置！）
  // 允许服务器重启时忽略内核 ehash 表中还处于 TIME_WAIT 状态（等待 2MSL）的旧连接，
  // 立刻绑定端口成功，彻底告别重启时的 "Address already in use" 报错。
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
  addr.sin_port =
      htons(8888); // Host to Network Short：主机小端序转网络大端序（16位端口）
  addr.sin_addr.s_addr =
      htonl(INADDR_ANY); // INADDR_ANY (0.0.0.0)：监听本机所有网卡（lo、内网、公网全收）

  if (bind(listen_fd, (const sockaddr *)&addr, sizeof(addr)) < 0) {
    std::perror("bind failed.");
    return -1;
  }

  // 4. 切换为被动监听状态 (LISTEN)，内核为其开辟【半连接队列】与【全连接队列 (Accept Queue)】
  // 注意：listen() 瞬间返回不阻塞！TCP 三次握手由内核在后台独立完成，握完手的连接排进全连接队列。
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

  // 将迎宾员 listen_fd 挂到 epoll 红黑树上，启用 EPOLLET 边缘触发：
  // 多批新连接同时完成三次握手进入全连接队列时，内核可能只通知一次 EPOLLIN，
  // 因此下方分支 A 必须用 while 循环把队列掏空到 EAGAIN，否则剩余连接会烂在队列里超时！
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = listen_fd; // 记录是谁触发了事件
  if (epoll_ctl(ep_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
    std::perror("epoll_ctl: ADD failed");
    return -1;
  }

  struct epoll_event events[64];
  std::unordered_map<int, Connection> connections;
  auto close_connection = [&](int fd) {
    if (epoll_ctl(ep_fd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
      std::perror("epoll_ctl: DEL failed");
    }

    close(fd);
    connections.erase(fd);
  };

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
        // 【ET 模式迎宾铁律】100 个连接同时到达可能只触发 1 次 EPOLLIN；
        // 必须循环调用 accept4() 直到返回 -1 且 errno == EAGAIN / EWOULDBLOCK，
        // 把内核全连接队列一次性全部掏空，否则剩下的连接会一直等不到服务而超时！
        while (true) {
          // 【关键陷阱防御】accept 创建的新 struct file 默认不会继承 listen_fd 的非阻塞属性！
          // 必须用 accept4(..., SOCK_NONBLOCK | SOCK_CLOEXEC) 一步到位把新连接焊死为非阻塞，
          // 否则遇到慢客户端时阻塞的 send() 会卡死整个事件循环。
          int conn_fd = accept4(listen_fd, nullptr, nullptr,
                                SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (conn_fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
              break; // 全连接队列已彻底掏空，正常退出接客循环
            }
            if (errno == EINTR)
              continue; // 被信号打断，队列可能还有客人，必须重试
            if (errno == EMFILE || errno == ENFILE) {
              // 【备胎救急闭环】进程 fd 配额耗尽时，用独立 tmp_fd 接收连接并立刻 close()：
              //   1) 绝不能直接用 idle_fd 接完不关就覆盖，否则该连接会成为无人知晓的“孤儿泄漏 fd”，
              //      客户端假活死等，备胎坑位也永久报废；
              //   2) 必须凭 accept() 的 tmp_fd >= 0（真的取走了连接）与 idle_fd != -1 决定是否继续，
              //      绝不能用 open() 的返回值判断“救急成功”，否则 accept 失败而 open 成功会 100% CPU 死循环！
              if (idle_fd != -1) {
                close(idle_fd);
                int tmp_fd = accept(listen_fd, nullptr, nullptr);
                if (tmp_fd >= 0) {
                  close(tmp_fd); // 1. 领出来立刻关掉劝退，把 fd 坑位腾回给 open
                }
                idle_fd =
                    open("/dev/null", O_RDONLY | O_CLOEXEC); // 2. 补回备胎
                if (tmp_fd >= 0 && idle_fd != -1) {
                  continue; // 3. 确实推进了队列且备胎已归位，才继续排空剩余积压连接
                }
              }
              break; // accept 没取走连接，或备胎已丢，立刻停手退出循环
            }
            std::perror("accept failed.");
            break;
          }

          // 为新客人建一本专属账本，初始关心 EPOLLIN | EPOLLRDHUP | EPOLLET
          // 切记：绝不能一上来就挂 EPOLLOUT！且必须与 Connection::events 初值严格一致。
          connections[conn_fd] = Connection();
          ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
          ev.data.fd = conn_fd;
          if (epoll_ctl(ep_fd, EPOLL_CTL_ADD, conn_fd, &ev) == -1) {
            std::perror("epoll_ctl: ADD failed");
            close(conn_fd);
            connections.erase(conn_fd);
          }
        }
        // ---------------- 分支 B：包厢服务员 (conn_fd) 亮灯，处理客户端读写 ----------------
      } else {
        uint32_t revents =
            events[i].events; // 本次实际亮起的指示灯位图 (Bitmask)
        auto it = connections.find(fd);
        if (it == connections.end()) {
          continue;
        }

        constexpr std::size_t kHighWaterMark = 64 * 1024; // 64KB 高水位线
        // 【核心不变式】“暂停读” = 未读到 EAGAIN/EOF 就提前停手，欠了这个连接一次通知。
        // 即使第 2 步 flush_output() 把积压发空导致期望掩码不变，第 3 步也必须靠它强制 MOD 补发通知！
        bool read_paused = false;
        auto &conn = it->second;

        // 尝试把 conn.output 持续塞进内核发送缓冲区，直到发空或撞上 EAGAIN。
        // 在 ET 模式下，send() 遇到 EINTR 也必须原地重试，否则若此时内核发送缓冲还未满
        // （未置 SOCK_NOSPACE），退出后就不会再收到下一次 EPOLLOUT 唤醒。
        // 返回 false 表示遇到 EPIPE / ECONNRESET 等硬错误，连接已被关闭。
        auto flush_output = [&]() -> bool {
          while (conn.sent < conn.output.size()) {
            // MSG_NOSIGNAL：如果对方已经断开连接，防止内核产生 SIGPIPE 信号杀死整个服务端进程
            ssize_t nwritten =
                send(fd, conn.output.data() + conn.sent,
                     conn.output.size() - conn.sent, MSG_NOSIGNAL);
            if (nwritten > 0) {
              conn.sent +=
                  nwritten; // 成功塞进内核 nwritten 字节，游标 O(1) 往前推
              if (conn.sent > conn.output.size() / 2) {
                // 已发废数据超过待发数据时才一次性 erase，将内存搬移开销摊还为 O(1)
                conn.output.erase(0, conn.sent);
                conn.sent = 0;
              }
            } else if (nwritten == -1 && errno == EINTR) {
              continue; // 被信号打断，写缓冲未满，必须继续写
            } else if (nwritten == -1 &&
                       (errno == EAGAIN || errno == EWOULDBLOCK)) {
              break; // 内核发送缓冲区真满了，剩下的留在 conn.output，交给第 3 步挂 EPOLLOUT
            } else {
              std::perror("send failed.");
              close_connection(fd);
              return false;
            }
          }
          return true;
        };

        // ==================== 1. 处理可读事件 (EPOLLIN) ====================
        // ET 模式下，内核只在新数据到达时挂链通知一次，因此必须 while 循环读到 EAGAIN 或 EOF(0)。
        if (revents & EPOLLIN) {
          char buff[1024 * 64];
          bool peer_error = false;

          while (true) {
            // 【边读边发 + 真积压高水位判定】
            // 不能还没调 send() 就拿 conn.output 直接和 64KB 比较，否则每次只读 1 次就会误触发停读！
            // 先试着调 flush_output() 往内核排一次：
            //   - 快客户端能立刻发空（unsent 归 0），继续抽干内核接收缓冲区，0 次额外 MOD；
            //   - 慢客户端若 send 撞上 EAGAIN 后仍积压 >= 64KB，才是“真背压”，置 read_paused 并停读。
            if (conn.output.size() - conn.sent >= kHighWaterMark) {
              if (!flush_output()) {
                peer_error = true;
                break;
              }
              if (conn.output.size() - conn.sent >= kHighWaterMark) {
                read_paused = true;
                break;
              }
            }

            ssize_t len = recv(fd, buff, sizeof(buff), 0);
            if (len > 0) {
              // 1a. 成功从内核接收缓冲区搬运了 len 字节，追加到应用层待发队列末尾
              conn.output.append(buff, len);
            } else if (len == 0) {
              // 1b. 返回 0 的唯一含义：对端发来了 FIN 包 (EOF)，且前面所有数据已读尽！
              // 【防截断关键】此时只代表客户端不再发新数据，不能当场 close(fd) 掀桌子，
              // 必须先打上 read_closed 标记，等下面把 conn.output 里的剩菜全发完再关！
              conn.read_closed = true;
              break;
            } else {
              // 1c. 返回 -1：区分假错误与真错误
              // 在 ET 模式下，遇到 EINTR 意味着还没见到 EAGAIN，接收缓冲区里可能还有余粮，
              // 绝不能 break 退出，必须 continue 重试！
              if (errno == EINTR) {
                continue;
              }
              if (errno == EAGAIN || errno == EWOULDBLOCK)
                break; // 内核 sk_receive_queue 已彻底抽干，正常退出读循环

              // ECONNRESET 等硬错误，关闭连接回收资源
              std::perror("recv failed. ");
              close_connection(fd);
              peer_error = true;
              break;
            }
          }

          if (peer_error) {
            continue;
          }
        }

        // ==================== 2. 处理发送与可写事件 (EPOLLOUT) ====================
        // 处理刚才读循环里未触发 64KB 阈值的尾包，或之前积压、被 EPOLLOUT 唤醒后的剩菜。
        // 注意：第 1 步与第 2 步是串行独立执行（并非 else if），因为同一轮可能两盏灯同时亮。
        if (!flush_output()) {
          continue;
        }

        // ==================== 3. 动态调整事件、流控背压与收尾 ====================
        std::size_t unsent =
            conn.output.size() - conn.sent; // 当前真正积压未发的字节数

        // 收尾检查：剩菜已经 100% 端上桌了，且客人已发来 FIN (read_closed) ──► 功德圆满，关连接！
        if (unsent == 0 && conn.read_closed) {
          close_connection(fd);
          continue;
        }

        // 所有积压数据均已发完，清空缓冲区并重置游标，及时释放内存
        if (unsent == 0 && !conn.output.empty()) {
          conn.output.clear();
          conn.sent = 0;
        }

        // 计算接下来希望监听的掩码：epoll_ctl(EPOLL_CTL_MOD) 是【全量覆盖】而非增量追加！
        // 因此每次都必须以 EPOLLET 为底色，并在保留读通道时带回 EPOLLRDHUP，
        // 否则一旦触发 MOD 就会把 EPOLLET 洗掉，连接意外退化回 LT 模式！
        uint32_t new_event = EPOLLET;
        if (unsent > 0) {
          // 还有剩菜没发完，挂上 EPOLLOUT 等对端读走数据腾出内核发送缓冲后再唤醒。
          // 什么时候必须摘掉 EPOLLIN（只留 EPOLLOUT）？
          //   情况 1 (conn.read_closed)：对端已关闭写端，不再关心新读事件；
          //   情况 2 (unsent >= kHighWaterMark)：【高水位背压 Backpressure】
          //          暂停 recv() 后内核接收队列很快填满，通过 TCP 零窗口 (ZeroWindow) 卡住慢客户端发送，
          //          防止用户态内存被撑爆 (OOM)。
          new_event |= (conn.read_closed || unsent >= kHighWaterMark)
                           ? EPOLLOUT
                           : (EPOLLOUT | EPOLLIN | EPOLLRDHUP);
        } else {
          // 没有积压数据，只监听可读与半关闭事件
          new_event |= (EPOLLIN | EPOLLRDHUP);
        }

        // 【消除冗余系统调用 + 消除微秒级竞态 + 保证调度公平】
        // 什么时候必须调用 epoll_ctl(EPOLL_CTL_MOD)？
        //   1. new_event != conn.events：监听掩码确实变化了（例如挂/摘 EPOLLOUT 或 EPOLLIN）；
        //   2. read_paused && (new_event & EPOLLIN)：修复“第 1 步刚因 send EAGAIN 暂停读，
        //      紧接着在进入第 2 步前的几微秒间隙里对端把数据抽空，导致第 2 步 flush_output()
        //      一把发完、unsent 归零、new_event 变回原掩码”的微秒级竞态！
        //      若此时跳过 MOD，内核 sk_receive_queue 里剩余数据在 ET 下将永远不会再唤醒。
        //      强制 MOD 会触发内核 ep_modify → ep_item_poll 复查状态，将连接重新排到 rdllist 队尾：
        //      既还清“暂停”欠下的那次通知，又把当前事件循环让给同批其他客户端，避免单连接饿死全服。
        if (new_event != conn.events ||
            (read_paused && (new_event & EPOLLIN))) {
          ev.events = new_event;
          ev.data.fd = fd;
          if (epoll_ctl(ep_fd, EPOLL_CTL_MOD, fd, &ev) == -1) {
            std::perror("epoll_ctl: MOD failed");
            close_connection(fd);
            continue;
          }
          // 必须在 epoll_ctl(MOD) 确认成功后再更新影子状态，防止内核修改失败导致用户态与红黑树失步！
          conn.events = new_event;
        }
      }
    }
  }
}
