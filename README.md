# amqp-demo-broker

内存中的单队列 AMQP 0-9-1 演示 broker。`pamqp` 只负责帧与方法编解码;
排队、确认与投递全部由 `broker.py` 自行管理。

## 限制与范围

| 项 | 值 |
| --- | --- |
| 连接数 | 最多 2 个(超出即拒绝) |
| 每连接通道数 | 最多 2 个(channel-max=2) |
| 队列 | 固定单个 `demo`,最多 20 条 |
| 消息体 | 每条至多 256 字节 |
| 持久化 / publisher confirms | 不支持 |

支持:连接与通道握手、固定队列 `queue.declare`、默认 exchange 的
`basic.publish`、`basic.consume`(手动确认)、`basic.qos`(prefetch 1~2)、
`basic.ack` / `basic.nack` / `basic.reject`、`basic.cancel`,以及通道与
连接的正常关闭。心跳未实现(协商为 0,客户端请禁用心跳)。

## 运行

```bash
docker compose up --build      # 仅部署此 broker,监听 5672
```

本地运行:`pip install -r requirements.txt && python broker.py`

## 测试

```bash
pip install -r requirements-dev.txt
pytest
```

* `tests/test_broker.py` — 真实 Pika 客户端:同号 delivery-tag 隔离、
  累计确认(multiple)、未知/重复标签只关闭肇事通道、断线归还、
  prefetch 信用、队列上限、连接/通道上限、正常关闭。
* `tests/test_frames.py` — 原始 socket 拆帧:跨 TCP 写拆分的协议头与帧、
  两个通道交错 publish、合并写入、超长/不足正文不成消息、
  断线未确认归还并标记 redelivered、broker 强制 channel-max。

## 关键设计

* **delivery-tag 按通道作用域**:未确认表挂在 `Channel.unacked` 上,
  两个通道可以同时持有 tag=1 而互不代表同一份确认凭证;
  `multiple` 累计确认只遍历本通道的未确认项。
* **按通道组装 publish**:方法帧、内容头、分段正文在所属通道上组装完整
  后才入队;其他通道的帧可任意穿插。连接中断时半成品直接丢弃;
  超过 256 字节的正文组装完成后丢弃,不成消息。
* **未知/重复标签**:只向该通道发 `channel.close (406)`,其未确认消息
  按首次入队序号重新排队并标记 `redelivered`,其他通道不受影响。
* **信用只在确认或归还后释放**:prefetch 余量 = `prefetch - len(unacked)`,
  ack/nack/通道关闭/连接关闭时才回收。
* **关闭的旧投递不再发送**:派发在事件循环内同步完成,写帧前重新检查
  通道与连接活性,已关闭的通道不会发出投递。
