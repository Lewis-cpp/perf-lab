#!/usr/bin/env python3
"""epoll echo 黑盒测试。

用法（在 server/ 目录下）：
  g++ -std=c++17 -g -Wall -Wextra -fsanitize=address,undefined -o build/echo_asan src/main.cpp
  ./build/echo_asan &
  python3 tests/test_echo.py $(pgrep -x echo_asan) [port]   # port 默认 8888

需要 server 的 PID：fd 泄漏和内存增长两项读取 /proc/<pid>/。全部通过时退出码为 0。
"""
import hashlib, os, selectors, socket, struct, sys, threading, time

PID = int(sys.argv[1])
ADDR = ("127.0.0.1", int(sys.argv[2]) if len(sys.argv) > 2 else 8888)


def conn(timeout=5):
    s = socket.create_connection(ADDR, timeout=timeout)
    s.settimeout(timeout)
    return s


def recv_n(s, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = s.recv(min(65536, n - len(buf)))
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


def nfds():
    return len(os.listdir(f"/proc/{PID}/fd"))


def rss_kb():
    with open(f"/proc/{PID}/status") as f:
        return next(int(l.split()[1]) for l in f if l.startswith("VmRSS"))


def test_basic():
    with conn() as s:
        s.sendall(b"hello\n")
        assert recv_n(s, 6) == b"hello\n"


def test_large_integrity():
    # 边发边收：一口气发完再收会撞上背压/阻塞问题
    data = os.urandom(1024 * 1024)
    with conn(10) as s:
        t = threading.Thread(target=s.sendall, args=(data,))
        t.start()
        got = recv_n(s, len(data))
        t.join()
    assert hashlib.sha256(got).digest() == hashlib.sha256(data).digest()


def test_many_clients():
    clients = [conn() for _ in range(200)]  # 超过 events[64]
    for i, s in enumerate(clients):
        s.sendall(f"c{i:04}".encode())
    for i, s in enumerate(clients):
        assert recv_n(s, 5) == f"c{i:04}".encode()
    for s in clients:
        s.close()


def test_1000_clients_large_concurrent():
    # W2 验收：1000 个连接同时在线，每个发 256KB 左右的不同数据（远超 64KB 高水位），边发边收，逐字节比对。
    # 用单线程 selectors 驱动全部连接：不能“先全发完再收”，那样两边的发送缓冲区都会被塞满，互相等着对方读，卡死。
    N, BASE = 1000, 256 * 1024
    big = memoryview(os.urandom(BASE + 2 * N))  # 共用一块随机数据，避免 1000 × 256KB 占满内存
    sel = selectors.DefaultSelector()
    state = {}
    for i in range(N):
        s = conn(10)
        s.setblocking(False)
        # 起点和长度都不同 → 每个连接的内容都不一样：串连接、错位、截断都能被查出来
        state[s] = {"data": big[i:i + BASE + i], "sent": 0, "got": 0, "id": i}
        sel.register(s, selectors.EVENT_READ | selectors.EVENT_WRITE)

    pending, deadline = N, time.time() + 60
    while pending:
        if time.time() > deadline:
            stuck = sorted(st["id"] for st in state.values() if st["got"] < len(st["data"]))
            raise AssertionError(f"{len(stuck)} 个连接超时未收全，例如 {stuck[:5]}")
        for key, ev in sel.select(timeout=1):
            s, st = key.fileobj, state[key.fileobj]
            data = st["data"]
            if ev & selectors.EVENT_WRITE and st["sent"] < len(data):
                try:
                    # 把剩余数据整段交给内核：数据一大批到达，然后客户端停下来等回显。
                    # ET 的“少读一次”或“欠一次通知”只有在没有后续数据补发通知时才会卡住；
                    # 如果每次只发 64KB 小块，新数据不断到来，会把这类 bug 掩盖掉。
                    st["sent"] += s.send(data[st["sent"]:])
                except BlockingIOError:
                    pass
                if st["sent"] == len(data):  # 发完就不再关心可写，否则 select 会一直报可写
                    sel.modify(s, selectors.EVENT_READ)
            if ev & selectors.EVENT_READ:
                try:
                    chunk = s.recv(65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    raise AssertionError(f"conn {st['id']} 被提前关闭，只收到 {st['got']}/{len(data)}")
                # 和期望的那一段直接比较，不用把收到的数据存下来（1000 × 96KB 太占内存）
                exp = data[st["got"]:st["got"] + len(chunk)]
                if chunk != exp:
                    raise AssertionError(f"conn {st['id']} 在偏移 {st['got']} 处数据不一致")
                st["got"] += len(chunk)
                if st["got"] == len(data):
                    sel.unregister(s)
                    s.close()
                    pending -= 1
    sel.close()


def test_half_close():
    with conn() as s:
        s.sendall(b"x" * 4096)
        s.shutdown(socket.SHUT_WR)
        assert recv_n(s, 8192) == b"x" * 4096


def test_half_close_large_delayed_read():
    # 发 4MB 后 SHUT_WR，先不读：server 收到 EOF 时 output 还有积压，不能丢
    data = os.urandom(4 * 1024 * 1024)
    s = conn(20)
    t = threading.Thread(target=lambda: (s.sendall(data), s.shutdown(socket.SHUT_WR)))
    t.start()
    time.sleep(1)
    got = recv_n(s, len(data) + 1)  # 多要 1 字节：确认之后是 EOF
    t.join()
    s.close()
    assert got == data, f"sent {len(data)} echoed {len(got)}"


def test_rst_and_abrupt_close():
    for _ in range(50):
        s = conn()
        s.sendall(b"y" * 8192)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        s.close()
    test_basic()


def test_no_fd_leak():
    time.sleep(0.2)
    before = nfds()
    for _ in range(500):
        with conn() as s:
            s.sendall(b"z")
            recv_n(s, 1)
    time.sleep(0.2)
    after = nfds()
    assert after == before, f"fd {before} -> {after}"


def test_slow_client_does_not_block_others():
    a = conn()
    a.settimeout(0.05)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            a.send(b"s" * 65536)
        except socket.timeout:
            break
    try:
        b = conn(timeout=2)
        b.sendall(b"ping")
        assert recv_n(b, 4) == b"ping"
        b.close()
    except (socket.timeout, TimeoutError):
        raise AssertionError("A 不读时 B 收不到回显：事件循环被卡住")
    finally:
        a.close()


def test_flood_memory_bounded():
    # 只发不收 5 秒：有背压时 server 内存应基本不涨
    before = rss_kb()
    s = conn()
    s.settimeout(0.2)
    end, pushed = time.time() + 5, 0
    while time.time() < end:
        try:
            pushed += s.send(b"f" * 65536)
        except socket.timeout:
            pass
    grew = rss_kb() - before
    s.close()
    assert grew < 50 * 1024, f"client pushed {pushed >> 20}MB, server RSS +{grew >> 10}MB"


failed = 0
for name, fn in list(globals().items()):
    if name.startswith("test_"):
        try:
            fn()
            print(f"PASS {name}")
        except Exception as e:
            failed += 1
            print(f"FAIL {name}: {type(e).__name__}: {e}")
sys.exit(1 if failed else 0)
