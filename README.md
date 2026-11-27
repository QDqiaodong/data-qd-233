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
`test_rollout_acceptance.py` 同样基于真实 `socketpair` 和事件门，覆盖不断线
换版、断线与迭代器清理对卸载的门控、首次工厂执行与发布交错、卸载未结束时
`Busy` 拒绝下一次发布，以及卸载异常经回执返回且不卡死后续发布。
`test_resource_gate_acceptance.py` 用真实 `socketpair`、事件门和有界等待覆盖
资源门：多键整组取得资格、执行位用尽时的跨连接仲裁（允许超车口径）、断线不丢弃
正在执行的隔离、未启用 gate 的逐调用拒绝，以及非法参数不占用客户端名额。
`test_shared_reads_acceptance.py` 同样用真实 `socketpair`、事件门和有界等待覆盖
同正文只读查询的物理读取合并：加入者绕过两项额度、每笔独立超时、最后等待者
离开后的口径、断线只撤销本连接成员、reader 失败/非法返回、未配置服务与本地
参数校验，并实际打印回调次数与超时观测。

应输出 `reply order: FAST SLOW` 和 `isolated error: handler failed`。

## 同正文只读查询合并（SharedReads）

多个监控组件经不同的本地长连接同时查询同一设备时，一次物理读取可以回答正文
完全相同的查询；但每个组件有自己的等待期限，一个组件等不及不能替别人放弃结果。
把这些连接挂到同一个 `SharedReads` 上即可：

```python
from rpc import Client, SharedReads, serve

def reader(payload, stop_event):        # 只读操作，返回至多 65536 字节 bytes
    return device.read(payload)

reads = SharedReads(reader, capacity=16)      # capacity 取 1..64 的整数

# 同一进程任意多个 serve 可共享同一个 reads；不同实例之间互不合并。
Thread(target=serve, args=(sock_1, handler),
       kwargs={"shared_reads": reads}).start()
Thread(target=serve, args=(sock_2, handler),
       kwargs={"shared_reads": reads}).start()
```

```python
future = client.submit_read(payload, wait_ms=2000)   # 与 submit() 一样不可取消
result = future.result(timeout=3)
```

- `payload` 是最多 65534 字节的 `bytes`（可为空），`wait_ms` 是 1 至 60000 的
  整数（不含 `bool`）。非法本地参数立即抛 `ValueError`，不发送帧、不占用请求 ID
  或 `max_pending` 在途名额。返回值与错误沿用普通调用的 `RESULT`/`ERROR` 契约。
- 只在同一个 `SharedReads` 内按正文 **bytes 完全相等** 合并；连接、等待期限和
  传输请求 ID 都不参与正文比较。未配置 `shared_reads` 的服务端对每次调用只回
  `RemoteError("shared reads unavailable")`，连接保持可用。
- 帧为新类型 `SHARED_READ`（见下表），旧调用方式与线格式不变。

### 一次物理读取，多个独立调用

- 服务端处理新请求时，若有正文相同的读取正在进行，就**加入**它：加入者既不需要
  全局读取额度，也不需要本连接的空闲执行位，只等结果，不再调用 reader。
- 否则只有**同时**满足“全局还有一个读取额度（`capacity`）”与“接单连接还有一个
  原有 `workers` 执行位”才开始新的物理读取；任一不满足立即
  `RemoteError("shared read busy")`，不排队、不调用 reader。
- 每份正文同时最多一个尚未退出的 reader。物理读取从开始到回调**实际退出**都占着
  上述两项额度；它不会因接单连接断开而挪用另一个连接的执行位。只读请求与普通
  请求、流、台账共同竞争同一条连接原有的执行位。
- 每笔请求的等待期限从服务端开始处理本请求起按单调时钟单独计算。到期只让这一笔
  收到 `RemoteError("shared read timed out")`，不必等 reader 返回；若读取在
  期限上或之后才决定完成，也不再覆盖该超时，即使超时回复尚未写出。回调抛异常或
  返回非法值时，仅向仍有资格的成员回 `RemoteError("handler failed")`。每笔请求至多
  一个终态。
- 服务端发现连接断开时立即撤掉该连接的成员，其客户端未完成请求按既有契约得到
  `PeerClosed`；别的连接上仍有效的成员继续等同一次读取。
- 已退出的物理读取**不**充当缓存，不为之后的请求保留历史。

### 最后等待者离开后的口径（本实现的选择）

规范允许两种口径，本库采用**允许重新加入**：

- 当因超时或断线不再有有效成员时，让 reader 继续运行、**不设置**停止信号；它尚未
  完成时，新到的同正文调用仍可加入并领取这次结果（即使当初只有它一个人等）。
- 只要还有有效成员，就不会因其他成员离开而触发任何停读动作。
- reader 实际退出后该次读取即结束，后来的同正文请求会开始一次全新的物理读取；
  不保留结果作为缓存。

若改用“停止接受加入”口径，则应请求 reader 停止，且在它实际退出前同正文请求收到
`shared read busy`，退出后才能发起新读取——本库**不**采用该口径，同一 `SharedReads`
实例始终使用上面的允许重新加入策略。

## 扩展帧类型

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

## 不断线换版与安全卸载（HandlerRollout）

同一进程的多条长连接可以共享一个 `HandlerRollout`，在客户不重连的情况下把
普通查询处理函数和流工厂作为一代组件整体换成新版，并由库保证旧组件在仍有
代码可能访问其资源时不会被卸载。

```python
from rpc import HandlerRollout, serve

rollout = HandlerRollout(handler_a, stream_a, on_retire=release_resources_a)

# rollout 可传给任意多条同时存在的连接；位置参数 handler 仍只服务台账指令。
Thread(target=serve, args=(sock_1, ledger_handler),
       kwargs={"rollout": rollout}).start()
Thread(target=serve, args=(sock_2, ledger_handler),
       kwargs={"rollout": rollout}).start()

# 管理线程发布新一代，并拿到被换下那一代的卸载回执 Future。
receipt = rollout.replace(handler_b, stream_b, on_retire=release_resources_b)
receipt.result()          # 旧代 on_retire 安全执行完后以 None 完成
```

- `HandlerRollout(handler, stream_handler, *, on_retire)` 与 `replace(...)` 的
  三个回调都必须可调用，否则抛 `ValueError` 且不改变现状。`serve(..., rollout=...)`
  由 rollout 提供普通查询与流工厂，因此不能再同时传非空的旧 `stream_handler`，
  否则启动时抛 `ValueError`；不传 `rollout` 的旧调用方式完全不变。
- `replace(...)` 把两类回调作为一代一起发布，立即返回代表**被换下那一代**的
  `concurrent.futures.Future`，该回执不可取消。发布不等待任何旧回调、连接退出
  或卸载完成；发布之后在任一既有或新连接上才接纳的普通查询与流都使用新代
  （再次换版则以换版次序为准）。已经接纳的普通查询始终由接纳时那一代处理。
  过载得到的 `server busy` 不是接纳，不钉住任何一代。
- **等待首次执行的流**：流在 `STREAM_OPEN` 接纳时只占流槽、不绑定组件；到它
  第一次真正获得执行位、调用工厂的时刻，才绑定当时有效的那一代。因此换版前
  已接纳但一直排队、没有运行过工厂的流，可能在换版后由新代工厂创建。规范允许
  在“接纳时”或“获得首次执行机会时”的有效组件中二选一，本库采用后者。无论选
  哪一代，一个流只调用一次工厂，从工厂开始到迭代器清理退出都钉在同一代，换版
  既不取消它也不重开它；排队本身不会被当成失败。
- **卸载口径**：旧代只有在再也不会进入它的任何普通处理函数、流工厂、推进或
  迭代器清理，且已经进入的这些工作全部退出后，才在 rollout 自有的守护线程上
  主动调用一次它的 `on_retire()`。卸载不占用各连接原有的 `workers` 执行位，
  也不要求客户重连、新代工作停止或管理线程再发命令；客户端放弃、连接断开或
  某一时刻没有代码在运行都不能单独作为可卸载凭据——例如暂停的流之后还能推进，
  断线流的迭代器可能仍在清理。回调正常返回后回执以 `None` 完成；回调抛异常时
  异常交给该回执，不回滚已发布的新版、不重试卸载，且该代代码不会再被调度。
- **并发换版**：每个 rollout 至多有一个已换下、尚未结束卸载的版本。上一份卸载
  回执未完成时再次（包括并发）调用 `replace` 立即抛 `rpc.Busy`，不改动当前
  版本、不排队，也不会调用失败提案中的任何回调。旧代卸载结束（即使因异常结束）
  后即可再次换版。库不保留已卸载组件的历史；业务调用与流仍沿用既有的执行位、
  流槽和窗口限制。
- 台账（`DeliveryLedger`）及补领行为不参与换版：台账指令始终使用 `serve`
  位置参数上的处理函数，rollout 不影响台账语义、线上帧或客户端。

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

## 按资源集合调度的互斥调用（ResourceGate）

多条长连接访问同一个设备配置服务时，普通 `submit()` 互不知情；需要跨连接
协调的多段读取/更新改用 `Client.submit_scoped()`，并在服务端把这些连接挂到
同一个 `ResourceGate` 上。资源键只是双方约定的 ASCII 名字，库不解析业务正文。

```python
from rpc import Client, ResourceGate, serve

def config_handler(payload):          # 与普通处理函数同形：收 bytes、回 bytes
    ...
    return result

gate = ResourceGate(config_handler, capacity=16)   # capacity 取 1..64 的整数

# 同一进程内任意多个 serve 可以共享同一个 gate；不同 gate 之间互不协调。
Thread(target=serve, args=(sock_1, ordinary_handler),
       kwargs={"resource_gate": gate}).start()
Thread(target=serve, args=(sock_2, ordinary_handler),
       kwargs={"resource_gate": gate}).start()
```

```python
future = client.submit_scoped(
    payload,
    shared=("section.b",),            # 共享声明：同键共享可并行
    exclusive=("section.a",),         # 独占声明：同键任一方独占即互斥
)
result = future.result(timeout=2)
```

- 两组键都是 tuple，合计 1 至 8 个且互不重复；每个键是 1 至 32 个 ASCII
  字符，只允许字母、数字、点、下划线和连字符，区分大小写。`payload` 是不超过
  65000 字节的 `bytes`。任何非法本地参数立即抛 `ValueError`，不发送帧、不占用
  请求 ID 或 `max_pending` 在途名额。
- 每次提交都是一次独立操作：库不按正文或键合并调用，断线后也不自动重发。
  返回的 future 与 `submit()` 一样不可取消，共用同一连接、请求 ID 序列和
  `max_pending`。
- 未启用 gate 的服务端对每次 scoped 调用单独回复
  `RemoteError("resource scheduling unavailable")`，连接和其他帧类型不受影响。

### 名额、执行位与回调归属

- gate 的 `capacity` 跨所有挂接连接合计，统计“已接单且尚未退出回调”的调用数
  （等待中的与运行中的都算）。接单时已满，该次立即得到
  `RemoteError("resource queue full")`，不留队列项、不调用回调。接单成功后
  暂时不能开始的调用保留原 future 排队等待，不会退回普通调用的
  `server busy`。
- scoped 回调由 **gate 自己的 handler** 提供，不改变普通、流、台账各自的回调
  选择，也不参与 `HandlerRollout` 换版。它运行在该连接原有的线程池上，并与普通
  处理、流的工厂/推进/清理、台账调用共同计入这条连接原有的 `workers` 执行位
  上限，不另加并发配额。
- 一个 scoped 调用只有在“整组键与正在运行的调用相容”且“本连接拿得到一个执行
  位”时才开始，并且这两项资格在同一步内一次取得；等待期间不占执行位，也不会
  只占住一组键的一半。共享声明之间同键可并行；同键只要任一方是独占就不能同时
  执行。
- 回调返回合法 bytes 即回复结果；回调抛异常或返回非法结果时该次得到
  `RemoteError("handler failed")`。资源键和 gate 名额在回调退出时即释放，不等
  客户端收取结果。

### 仲裁口径（本实现的选择）

规范允许两种口径，本库采用**允许超车**：

- 仲裁按 gate 接单顺序扫描队列，启动所有“整组键与运行集合相容且本连接当前有
  空闲执行位”的调用。
- 因键冲突而等待的较早调用**不会封住自己声明的其他空闲键**：与它不冲突的调用
  不会仅因排在它后面而停住。
- 仅因本连接执行位用尽而等待的较早调用，手上没有键也没有执行位；此时一个能
  立即满足条件的后到冲突调用可以先执行（超车）。较早调用一旦其连接释放执行位、
  且键集合重新相容，就会在后续扫描中启动。
- 在没有新增竞争、所需执行位与键已释放、对端仍在读帧、已开始的回调都会返回的
  前提下，每个已接单调用都会继续推进直至结束，不会永久饥饿。

### 断线时哪些工作仍会执行

- 服务端发现某连接断开时，立即丢弃该连接**尚未取得执行资格**的排队项，释放其
  gate 等待名额与任何排队位置；这些调用在客户端按既有 `PeerClosed` 契约结束。
- **已经取得执行资格**的 scoped 操作会继续执行到回调退出，期间它声明的键不会
  因连接消失而提前释放；只是回复帧无人接收。因此别的连接上同键的新调用仍须等
  旧回调真正退出。
- gate 不保留已结束调用的历史，无磁盘状态，也不提供进程重启后的恢复。

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
| `SCOPED_REQUEST` | 11 | C→S | 共享、独占计数各 1 字节，随后每个键为 1 字节长度加最多 32 字节 ASCII 名（先共享后独占，合计 1..8 个互不重复键），最后是最多 65000 字节正文；回复复用 `RESULT`/`ERROR` |
| `SHARED_READ` | 12 | C→S | 2 字节大端等待毫秒数（1..60000），后接最多 65534 字节正文（可为空）；回复复用 `RESULT`/`ERROR` |

没有远端自动重连或身份认证；处理函数和 future 回调由调用方信任，且必须
能够返回。回调不要阻塞、同步等待其他 RPC 结果或再次提交 RPC。底层对端
完全停止读取套接字时，不保证服务端的 `sendall` 能立即返回；正常
`Client` 读取线程会持续收帧并按窗口缓冲或丢弃已放弃流的数据。
