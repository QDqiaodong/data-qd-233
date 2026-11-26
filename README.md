# 本地流套接字 RPC

本地受信组件把已连接的流套接字交给 `Client` 和 `serve`，在同一连接上
提交多个二进制调用，或打开可逐块消费的流式回复。`submit()` 返回 future，
普通回复按请求 ID 归属，不必按发送顺序完成。客户端在途上限和服务端工作位
分别限流；服务端忙或处理失败只结束该调用，连接中断则结束全部未完成调用。

离线演示在真实 `socketpair` 上用自制字节验证乱序完成及单调用失败：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m pip check
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

当前环境若已有 Python 3.9+，也可直接运行：

```sh
python3 -m unittest -v
python3 demo.py
```

`test_rpc.py` 保留普通调用回归；`test_streams_acceptance.py` 全部使用真实
`socketpair` 和事件门，覆盖题目要求的三个时序场景，以及前缀错误隔离、
窗口上限、未配置流处理器和断线清理。`test_delivery_acceptance.py` 用真实
`socketpair` 和事件门覆盖补领台账：断线后换连接补领、容量/执行位门控、
冲突与待完成、失败终态和未启用台账时的拒绝。

应输出 `reply order: FAST SLOW` 和 `isolated error: handler failed`。

## 普通调用

```python
future = client.submit(b"request")
result = future.result(timeout=2)
```

帧为 1 字节类型、8 字节无符号请求 ID、4 字节负载长度（大端），再跟最多
65536 字节的负载。请求 ID 按发送顺序递增，回复 ID 必须对应仍在途的调用。
传入阻塞套接字并转移其所有权；发送受套接字背压，future 超时只结束调用方
等待。

服务端过载时，新的普通调用立即得到 `RemoteError("server busy")`；单个
handler 抛出异常只令该调用得到 `RemoteError("handler failed")`。

## 流式回复

```python
with client.open_stream(b"collect", window=1) as stream:
    while True:
        chunk = stream.read(timeout=2)
        if chunk is None:
            break
        process(chunk)
    stream.wait_closed(timeout=2)
```

- `open_stream(payload, *, window=1)` 的 `payload` 限制与 `submit()` 相同；
  `window` 必须是 1 到 16 的整数。
- 每个块是最多 65536 字节的 `bytes`；`b""` 也是正常数据。
- 正常迭代结束后，`read()` 按序交付剩余前缀，然后持续返回 `None`。
- `read(timeout=...)` 超时抛出内置 `TimeoutError`；本次等待结束，但不关闭
  流、不消耗数据，下一次 `read()` 仍从同一位置继续。
- 每个句柄至多一个并行 `read()`；另一个线程可以调用 `close()`。
- `close()` 可重复调用，只放弃当前流。本地放弃立即生效：等待中的及后续的
  `read()` 都抛出 `StreamClosed`，未读块被丢弃；它不会等待服务端生产器
  返回。
- `wait_closed(timeout=None)` 等待远端结算。正常 EOF、远端错误或放弃后的
  服务端确认都会令其返回 `None`；超时抛出 `TimeoutError`，断线则抛出
  `PeerClosed`。它不替应用读取或推进已缓冲的数据。

服务端写法：

```python
def stream_handler(payload):
    yield b"first"
    yield b""
    yield b"last"

serve(
    sock,
    handler,
    workers=4,
    stream_handler=stream_handler,
    stream_slots=8,   # 1..32
)
```

`stream_handler(payload)` 返回 Python 迭代器；迭代正常结束即 EOF。生产器
抛出异常或产生非 `bytes`/超过 65536 字节的块时，只失败该流：此前已合法
发送的前缀仍可按序读取，之后的 `read()` 抛出 `RemoteError`。

未配置 `stream_handler` 时，流读取得到
`RemoteError("streaming unavailable")`。所有流槽都被占用时，新流立即得到
`RemoteError("server streams busy")`，且不会调用生产器；它与普通调用的
`server busy` 相互独立。

## 断线后补领回执（台账）

普通 `submit()`、`open_stream()` 和旧的三种帧不使用台账。若调用会带来副作用、
且连接可能在处理函数已经开始后被断开，则改用带业务编号的补领接口：

```python
from rpc import Client, DeliveryLedger, serve

ledger = DeliveryLedger(capacity=8)          # 1..64 条未确认指令

def handler(payload):                        # 同一个普通处理函数
    ...

server = Thread(
    target=serve,
    args=(sock, handler),
    kwargs={"ledger": ledger},               # 可选关键字参数
)
```

```python
future = client.submit_delivery(seq, payload)   # 返回 future
result = future.result(timeout=2)
client.confirm_delivery(seq).result(2)          # 确认收讫，释放名额
```

- `seq` 是独立的业务编号，必须为 1 到 2⁶⁴−1 的整数（`bool` 不接受）；正文
  是最多 65528 字节的 `bytes`。非法本地参数立即抛 `ValueError`，不发送帧、
  不占用在途名额。
- 两个方法都像 `submit()` 一样返回 future，并共用客户端原有的
  `max_pending` 在途上限和请求 ID 序列。
- 未配置台账的服务端对这两个方法回复
  `RemoteError("delivery unavailable")`，连接保持可用。

用一条 `DeliveryLedger` 表示同一生产者的一条指令序列；多个同时存在或先后
接入的连接可以共享它，并使用同一个业务处理函数。一条指令被接单后，台账
保证它恰好执行一次，即使原连接已经断开。

### 何时补领、何时确认

生产者拿到终态（`future` 的正常结果，或 `handler failed` 这类
`RemoteError`）后即可调用 `confirm_delivery(seq)` 释放名额。若连接在处理
过程中断开、`future` 得到 `PeerClosed`，生产者重连后应先用原编号和原正文
再次 `submit_delivery(seq, payload)` 补领：

- 正文相同：已接单的指令按台账处理。仍在运行可能立即得到
  `"delivery pending"`（稍后再补领），也可能保留该请求等到终态，并在同一次
  future 上收到同一个终态；已有终态则直接返回原结果。
- `"delivery conflict"`：编号已接单但正文不同，原记录不变。
- `"delivery retired"`：编号已被确认或曾跳过，不会再次执行。
- `"delivery full"`：台账未确认记录已达 `capacity`。
- `"server busy"`：台账有名额，但当前连接没有空闲执行位。
- `"delivery unknown"`：确认一个从未接单、且大于最高已接单编号的序列。

确认运行中的指令得到 `"delivery pending"` 且不释放名额；确认已完成的指令
返回空 `bytes` 并释放名额。对已确认或曾跳过的编号再次确认幂等返回空
`bytes`，不会挤掉其他编号的记录。

### 名额与执行位

已接单而未确认的指令始终占一个台账名额；台账最多保存 `capacity` 条这样的
记录（正文和终态各不超过一个普通帧的负载上限），另加最高已接单编号等少量
固定状态，不保存完整历史。新指令只有在编号大于最高已接单编号、台账有名额
且当前连接有空闲执行位时才接单并调用处理函数；未接单不会推进最高编号、
占用台账名额或调用处理函数，之后可用同一编号重试。

台账只对已接单指令复用当前连接原有的 `workers` 执行位，不另加并发能力；
补领与确认不占执行位，也不占用等待中的请求。台账只在当前进程内有效：
进程重启后 `DeliveryLedger` 为空，服务端无法识别重启前已执行的编号，因此
不提供跨重启的去重保证；它也不做磁盘恢复、自动重连或自动重试——重连和
补领由生产者主动完成。

## 执行位、窗口和名额

普通 handler，以及流生产器的创建、每次 `next()` 推进和迭代器 `close()`
清理，共同使用 `workers` 个执行位；同一个迭代器的这些操作由同一运行线程
串行执行，不会重叠。仅因读取端未取走块而暂停的流继续占用 `stream_slots`
中的流槽，但不占用 `workers` 执行位。已接纳的流在等待空闲执行位时不会被
当作没有流槽拒绝。

每条流最多有 `window` 个尚未交付给 `read()` 的块/推进份额，包含正在发送、
网络在途和客户端已缓冲的块。客户端只在 `read()` 真正取走一个块后归还一个
信用份额；没有份额时服务端不会再次推进迭代器。实现可以提前缓冲至多窗口内
的块，但不要求必须预取满窗口。

流与普通调用共享同一条连接、同一个单调请求 ID 序列和客户端
`max_pending`。流占用的名额在“远端已结算”并且“客户端没有未读块”后才释放；
本地 `close()` 会丢弃未读块，但名额仍保留到远端结算。因此
`max_pending=1` 时，本地放弃后、服务端确认前提交普通调用仍会在客户端得到
`Busy`。

## 放弃、断线和帧归属

放弃后到达的该流数据帧仍能按请求 ID 识别并丢弃，不会变成未知回复，也不会
影响同一连接上的其他调用。客户端只在远端结算前保留该流的少量状态；不保存
历次调用的结束 ID 表，也不使用无界队列。

若迭代器提供 `close()`，在本地放弃、迭代失败或连接停止时，服务端会等当前
一次用户操作退出，并在当前执行位上恰好清理一次；服务端不会强杀或中断用户
代码。只有清理完成且流槽释放后，服务端才发送结算帧。

连接断开时，尚未收到最终结果的普通 future、流 `read()` 和
`wait_closed()` 都收到 `PeerClosed`；服务端套接字停止后会唤醒并清理已停下
的生产器。已经收到的普通结果或已经结算的流状态不因之后断线而改变。

## 扩展帧类型

旧的三种帧类型和线格式保持不变：

| 类型 | 值 | 方向 | 负载 |
| --- | ---: | --- | --- |
| `REQUEST` | 1 | C→S | 普通请求负载 |
| `RESULT` | 2 | S→C | 普通结果负载 |
| `ERROR` | 3 | S→C | UTF-8 错误消息 |

流扩展复用同一个 `>BQI` 定长头：

| 类型 | 值 | 方向 | 负载 |
| --- | ---: | --- | --- |
| `STREAM_OPEN` | 4 | C→S | 1 字节窗口值，后接普通请求负载 |
| `STREAM_DATA` | 5 | S→C | 一个最多 65536 字节的块，长度可为 0 |
| `STREAM_END` | 6 | S→C | 空；正常 EOF 或放弃确认 |
| `STREAM_CANCEL` | 7 | C→S | 空；本地放弃 |
| `STREAM_CREDIT` | 8 | C→S | 空；`read()` 取走一块后归还一个窗口份额 |
| `DELIVERY_SUBMIT` | 9 | C→S | 8 字节大端序列号后接最多 65528 字节正文；回复复用 `RESULT`/`ERROR` |
| `DELIVERY_CONFIRM` | 10 | C→S | 恰好 8 字节大端序列号；回复复用 `RESULT`/`ERROR` |

没有远端自动重连或身份认证；处理函数和 future 回调由调用方信任，且必须
能够返回。回调不要阻塞、同步等待其他 RPC 结果或再次提交 RPC。底层对端
完全停止读取套接字时，不保证服务端的 `sendall` 能立即返回；正常
`Client` 读取线程会持续收帧并按窗口缓冲或丢弃已放弃流的数据。
