# 本地流套接字 RPC

本地受信组件把已连接的流套接字交给 `Client` 和 `serve`，在同一连接上
提交多个二进制调用；`submit()` 返回 future，回复按请求 ID 归属，不必按
发送顺序完成。客户端在途上限和服务端工作位分别限流；服务端忙或处理失败
只结束该调用，连接中断则结束全部未完成调用。

离线演示在真实 socketpair 上用自制字节验证乱序完成及单调用失败：

Python 3.9+，仅标准库。从仓库根目录运行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m pip check
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

应输出 `reply order: FAST SLOW` 和 `isolated error: handler failed`。
帧为 1 字节类型、8 字节无符号请求 ID、4 字节负载长度（大端），再跟最多
65536 字节的负载。请求 ID 按发送顺序递增，回复 ID 必须对应仍在途的调用。
传入阻塞套接字并转移其所有权；发送受套接字背压，future 超时只结束调用方等待。
没有远端取消、自动重连或身份认证；处理函数和 future 回调由调用方信任，
且必须返回。回调不要阻塞、同步等待其他 RPC 结果或再次提交 RPC。
