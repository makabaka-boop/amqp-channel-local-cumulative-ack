"""Frame-level tests: raw sockets, fragmented/interleaved/coalesced frames."""

from pamqp import commands as spec
from pamqp.body import ContentBody
from pamqp.header import ContentHeader

from conftest import RawClient, Recorder, pump_until, pika_connect, wait_for


def test_broker_enforces_channel_limit(server):
    """A client that ignores channel-max=2 gets its connection closed."""
    raw = RawClient(server.port)
    raw.handshake()
    raw.open_channel(1)
    raw.open_channel(2)
    raw.write_frame(spec.Channel.Open(), 3)
    channel, close = raw.read_frame()
    assert channel == 0
    assert isinstance(close, spec.Connection.Close)
    assert close.reply_code == 504
    raw.write_frame(spec.Connection.CloseOk(), 0)
    raw.close()
    wait_for(lambda: len(server.broker.connections) == 0)


def test_protocol_header_and_frames_split_across_tcp_writes(server):
    raw = RawClient(server.port)
    raw.handshake(fragment_header=True)
    # Channel.Open dribbled out three bytes at a time
    raw.write_frame_fragmented(spec.Channel.Open(), 1, chunk=3, delay=0.01)
    channel, obj = raw.read_frame()
    assert channel == 1 and isinstance(obj, spec.Channel.OpenOk)
    raw.declare(1)
    raw.close()


def test_interleaved_publish_on_two_channels(server):
    """A publish is assembled per channel: frames from another channel may
    interleave, and each message is enqueued only once fully assembled."""
    raw = RawClient(server.port)
    raw.handshake()
    raw.open_channel(1)
    raw.open_channel(2)

    body1 = bytes(range(100))  # 100-byte message on channel 1
    body2 = b"B" * 50  # 50-byte message on channel 2

    # channel 1 starts a publish (method + header + first body fragment)
    raw.write_frame(spec.Basic.Publish(exchange="", routing_key="demo"), 1)
    raw.write_frame(
        ContentHeader(body_size=len(body1), properties=spec.Basic.Properties()), 1
    )
    raw.write_frame(ContentBody(body1[:30]), 1)

    # channel 2 interleaves a complete publish, coalesced into one TCP write
    raw.publish(2, body2, coalesce=True)

    # channel 1 finishes; its body frame is fragmented into 5-byte TCP writes
    raw.write_frame_fragmented(ContentBody(body1[30:70]), 1, chunk=5)
    raw.write_frame(ContentBody(body1[70:]), 1)

    # channel 2's publish completed first, so it holds seq 1
    declared = raw.declare(1)
    assert declared.message_count == 2

    conn = pika_connect(server.port)
    ch = conn.channel()
    ch.queue_declare(queue="demo")
    ch.basic_qos(prefetch_count=2)
    rec = Recorder(ch)
    pump_until(conn, lambda: len(rec.events) == 2)
    assert [e.body for e in rec.events] == [body2, body1]  # completion order
    rec.ack(tag=2, multiple=True)
    wait_for(lambda: len(server.broker.messages) == 0)
    conn.close()
    raw.close()


def test_oversized_body_never_becomes_a_message(server):
    raw = RawClient(server.port)
    raw.handshake()
    raw.open_channel(1)

    big = b"x" * 300  # exceeds the 256-byte limit
    raw.publish(1, big, body_chunk=64)  # fully assembled, then must be dropped
    assert raw.declare(1).message_count == 0

    raw.publish(1, b"ok")
    assert raw.declare(1).message_count == 1

    conn = pika_connect(server.port)
    ch = conn.channel()
    ch.queue_declare(queue="demo")
    ch.basic_qos(prefetch_count=1)
    rec = Recorder(ch)
    pump_until(conn, lambda: len(rec.events) == 1)
    assert rec.events[0].body == b"ok"
    rec.ack(0)
    conn.close()
    raw.close()


def test_incomplete_publish_discarded_on_disconnect(server):
    raw = RawClient(server.port)
    raw.handshake()
    raw.open_channel(1)
    raw.write_frame(spec.Basic.Publish(exchange="", routing_key="demo"), 1)
    raw.write_frame(
        ContentHeader(body_size=50, properties=spec.Basic.Properties()), 1
    )
    raw.write_frame(ContentBody(b"x" * 20), 1)  # 30 bytes short, then vanish
    raw.close()

    wait_for(lambda: len(server.broker.connections) == 0)
    assert len(server.broker.messages) == 0

    conn = pika_connect(server.port)
    ch = conn.channel()
    assert ch.queue_declare(queue="demo").method.message_count == 0
    conn.close()


def test_same_tag_on_two_channels_and_duplicate_tag_error(server):
    """Frame-level pinpoint: both channels hold delivery-tag 1; ack(1) on
    channel 1 must not touch channel 2's delivery; a duplicate ack(1) on
    channel 1 closes only channel 1."""
    raw = RawClient(server.port)
    raw.handshake()
    raw.open_channel(1)
    raw.open_channel(2)
    raw.qos(1, 2)
    raw.qos(2, 2)
    raw.consume(1)
    raw.consume(2)

    conn = pika_connect(server.port)
    pub = conn.channel()
    pub.queue_declare(queue="demo")
    pub.basic_publish(exchange="", routing_key="demo", body=b"m0")
    pub.basic_publish(exchange="", routing_key="demo", body=b"m1")

    deliveries = {}
    for _ in range(2):
        channel, deliver, body = raw.read_delivery()
        deliveries[channel] = (deliver, body)
    assert deliveries[1][0].delivery_tag == 1
    assert deliveries[2][0].delivery_tag == 1
    bodies = {deliveries[1][1], deliveries[2][1]}
    assert bodies == {b"m0", b"m1"}

    # ack tag 1 on channel 1, then tag 1 on channel 2: both are valid,
    # channel-scoped credentials
    raw.write_frame(spec.Basic.Ack(delivery_tag=1, multiple=False), 1)
    raw.write_frame(spec.Basic.Ack(delivery_tag=1, multiple=False), 2)
    declared = raw.declare(2)  # channel 2 alive and well
    assert declared.message_count == 0

    # duplicate ack of tag 1 on channel 1: only channel 1 is closed
    raw.write_frame(spec.Basic.Ack(delivery_tag=1, multiple=False), 1)
    channel, close = raw.read_frame()
    assert channel == 1
    assert isinstance(close, spec.Channel.Close)
    assert close.reply_code == 406
    raw.write_frame(spec.Channel.CloseOk(), 1)

    # channel 2 and the connection keep working
    assert raw.declare(2).message_count == 0
    conn.close()
    raw.close()


def test_abrupt_disconnect_returns_unacked_redelivered(server):
    raw = RawClient(server.port)
    raw.handshake()
    raw.open_channel(1)
    raw.qos(1, 2)
    raw.consume(1)

    conn = pika_connect(server.port)
    pub = conn.channel()
    pub.queue_declare(queue="demo")
    for body in (b"m0", b"m1", b"m2"):
        pub.basic_publish(exchange="", routing_key="demo", body=body)

    _, d1, b1 = raw.read_delivery()
    _, d2, b2 = raw.read_delivery()
    assert (d1.delivery_tag, b1) == (1, b"m0")
    assert (d2.delivery_tag, b2) == (2, b"m1")
    raw.close()  # vanish without acking: m0 and m1 must be returned

    wait_for(lambda: len(server.broker.messages) == 3)

    ch = conn.channel()
    ch.basic_qos(prefetch_count=2)
    rec = Recorder(ch)
    pump_until(conn, lambda: len(rec.events) == 2)
    assert [e.body for e in rec.events] == [b"m0", b"m1"]
    assert [e.redelivered for e in rec.events] == [True, True]
    rec.ack(tag=2, multiple=True)
    pump_until(conn, lambda: len(rec.events) == 3)
    assert rec.events[2].body == b"m2"
    assert rec.events[2].redelivered is False
    rec.ack(2)
    wait_for(lambda: len(server.broker.messages) == 0)
    conn.close()
