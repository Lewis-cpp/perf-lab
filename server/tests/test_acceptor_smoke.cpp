// Acceptor 冒烟测试：三个场景，每个场景用独立的 EventLoop（quit_ 置位后不可重置）。
//
// 构建与运行（在 server/ 目录下）：
//   cmake -S . -B build && cmake --build build --target acceptor_smoke && ./build/acceptor_smoke
//
// 1. ET 排空：loop 启动前先让 50 个连接在全连接队列里握完手。ET 只给一次 EPOLLIN，
//    handleRead 若没循环到 EAGAIN，剩下的连接再也等不到通知 → 卡住，被 alarm 判失败。
// 2. 无人接手：不设回调，Acceptor 必须自己 close，客户端应读到 EOF。
// 3. EMFILE 救急：fd 用尽时 3 个客户端排队，Acceptor 应靠 idleFd_ 逐个“领出来就关”，
//    回调 0 次、不死循环；客户端 1 读到 EOF 后释放 fd，新连接应能正常交给回调（恢复）。
#include "net/Acceptor.h"
#include "net/Channel.h"
#include "net/EventLoop.h"

#include <arpa/inet.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);     \
      std::exit(1);                                                            \
    }                                                                          \
  } while (0)

static void onAlarm(int) {
  const char msg[] = "FAIL: timeout (loop never quit: ET not drained or busy loop)\n";
  (void)!write(2, msg, sizeof(msg) - 1);
  _exit(1);
}

// 阻塞 connect：握手由内核完成，不需要服务端先 accept
static int connectTo(uint16_t port) {
  int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  CHECK(fd >= 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  CHECK(connect(fd, (sockaddr *)&addr, sizeof(addr)) == 0);
  return fd;
}

// 服务端关掉了连接 → 客户端 recv 返回 0（EOF）。最多等 2 秒。
static bool seesEof(int fd) {
  timeval tv{2, 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  char c;
  return recv(fd, &c, 1, 0) == 0;
}

static void testEtDrain() {
  const uint16_t port = 18901;
  const int kClients = 50; // < backlog 128，全部能进全连接队列
  EventLoop loop;
  Acceptor acceptor(&loop, port);
  int accepted = 0;
  acceptor.setNewConnectionCallback([&](int fd) {
    ::close(fd);
    if (++accepted == kClients) loop.quit();
  });
  acceptor.listen();

  std::vector<int> clients;
  for (int i = 0; i < kClients; ++i) clients.push_back(connectTo(port));

  alarm(5);
  loop.loop();
  alarm(0);
  CHECK(accepted == kClients);
  for (int fd : clients) ::close(fd);
  std::printf("PASS et_drain: %d/%d accepted\n", accepted, kClients);
}

static void testNoCallbackCloses() {
  const uint16_t port = 18902;
  EventLoop loop;
  Acceptor acceptor(&loop, port); // 故意不设回调
  acceptor.listen();
  int client = connectTo(port);

  // 用 pipe 让 loop 在同一轮里退出：listen fd 和 pipe 同时就绪
  int p[2];
  CHECK(pipe2(p, O_NONBLOCK | O_CLOEXEC) == 0);
  Channel quitCh(&loop, p[0]);
  quitCh.setReadCallback([&] { loop.quit(); });
  CHECK(quitCh.enableReading());
  CHECK(write(p[1], "q", 1) == 1);

  alarm(5);
  loop.loop();
  alarm(0);
  CHECK(seesEof(client));

  quitCh.remove();
  for (int fd : {p[0], p[1], client}) ::close(fd);
  std::printf("PASS no_callback: client got EOF\n");
}

static void testEmfileRescue() {
  const uint16_t port = 18903;
  EventLoop loop;
  Acceptor acceptor(&loop, port);
  int accepted = 0;
  std::vector<int> fillers;

  acceptor.setNewConnectionCallback([&](int fd) {
    ::close(fd);
    ++accepted;
    loop.quit(); // 只有恢复阶段的新连接会走到这里
  });
  acceptor.listen();

  // fd 用尽之前先建好 3 个客户端（客户端 fd 也占本进程配额）
  int c1 = connectTo(port), c2 = connectTo(port), c3 = connectTo(port);

  // 客户端 1 读到 EOF 说明 Acceptor 已经“领出来就关”。此时释放填充 fd，再发起一个新连接
  int extra = -1;
  int acceptedDuringEmfile = -1;
  Channel c1Ch(&loop, c1);
  c1Ch.setReadCallback([&] {
    if (acceptedDuringEmfile != -1) return; // 只处理第一次
    acceptedDuringEmfile = accepted;
    for (int fd : fillers) ::close(fd);
    fillers.clear();
    extra = connectTo(port);
  });
  CHECK(c1Ch.enableReading());

  // 压低上限并用 dup 填满：之后 accept4 必然 EMFILE
  rlimit old{};
  CHECK(getrlimit(RLIMIT_NOFILE, &old) == 0);
  rlimit low = old;
  low.rlim_cur = 64;
  CHECK(setrlimit(RLIMIT_NOFILE, &low) == 0);
  for (int fd; (fd = dup(0)) >= 0;) fillers.push_back(fd);

  alarm(5);
  loop.loop();
  alarm(0);
  CHECK(setrlimit(RLIMIT_NOFILE, &old) == 0);

  CHECK(acceptedDuringEmfile == 0); // fd 用尽期间回调一次都没被调用
  CHECK(seesEof(c2));               // 排队的另外两个也被劝退了
  CHECK(seesEof(c3));
  CHECK(accepted == 1); // 恢复后新连接正常交给回调

  c1Ch.remove();
  for (int fd : {c1, c2, c3, extra}) ::close(fd);
  std::printf("PASS emfile_rescue: 3 queued clients got EOF, recovered and accepted 1\n");
}

int main() {
  std::signal(SIGALRM, onAlarm);
  testEtDrain();
  testNoCallbackCloses();
  testEmfileRescue();
  std::printf("ALL PASS\n");
}
