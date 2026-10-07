"""Integration tests driving the broker with real pika clients."""

import time

import pika
import pytest

from conftest import Recorder, pump_until, pika_connect, wait_for


def broker_channels(server):
    """Open broker-side channels keyed by (connection-ident-order, channel)."""
    conns = sorted(server.broker.connections, key=lambda c: c.ident)
    return [conn.channels for conn in conns]


def test_handshake_declare_publish_consume_roundtrip(server):
    conn = pika_connect(server.port)
    ch = conn.channel()
    result = ch.queue_declare(queue="demo")
    assert result.method.message_count == 0
    ch.basic_qos(prefetch_count=2)

    bodies = [b"hello-%d" % i for i in range(3)]
    props = pika.BasicProperties(content_type="text/plain", message_id="m-0")
    for body in bodies:
        ch.basic_publish(exchange="", routing_key="demo", body=body, properties=props)

    rec = Recorder(ch)
    pump_until(conn, lambda: len(rec.events) == 2)  # prefetch=2
    assert [e.body for e in rec.events] == bodies[:2]
    assert [e.tag for e in rec.events] == [1, 2]
    assert rec.events[0].properties.content_type == "text/plain"
    rec.ack(0)
    pump_until(conn, lambda: len(rec.events) == 3)
    assert rec.events[2].body == bodies[2]
    rec.ack(1)
    rec.ack(2)

    wait_for(lambda: len(server.broker.messages) == 0)
    result = ch.queue_declare(queue="demo")
    assert result.method.message_count == 0
    conn.close()
    wait_for(lambda: len(server.broker.connections) == 0)


def test_declare_only_fixed_queue(server):
    conn = pika_connect(server.port)
    ch = conn.channel()
    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exc_info:
        ch.queue_declare(queue="somewhere-else")
    assert exc_info.value.reply_code == 406
    # connection survives; a fresh channel still works
    ch2 = conn.channel()
    assert ch2.queue_declare(queue="demo").method.message_count == 0
    conn.close()


def test_same_delivery_tag_on_two_channels_is_isolated(server):
    """Both consumers may hold delivery-tag 1 at once; acking tag 1 on one
    channel must not settle or drop the other channel's delivery."""
    conn_a = pika_connect(server.port)
    conn_b = pika_connect(server.port)
    ch_a = conn_a.channel()
    ch_b = conn_b.channel()
    ch_a.queue_declare(queue="demo")
    ch_b.queue_declare(queue="demo")
    ch_a.basic_qos(prefetch_count=2)
    ch_b.basic_qos(prefetch_count=2)
    rec_a = Recorder(ch_a)
    rec_b = Recorder(ch_b)

    pub = conn_a.channel()  # second channel on connection A
    bodies = [b"msg-%d" % i for i in range(4)]
    for body in bodies:
        pub.basic_publish(exchange="", routing_key="demo", body=body)

    pump_until([conn_a, conn_b], lambda: len(rec_a.events) == 2 and len(rec_b.events) == 2)
    # round-robin: A got msg-0/tag1 + msg-2/tag2, B got msg-1/tag1 + msg-3/tag2
    assert [e.tag for e in rec_a.events] == [1, 2]
    assert [e.tag for e in rec_b.events] == [1, 2]

    channels = broker_channels(server)
    bk_a, bk_b = channels[0][1], channels[1][1]
    assert set(bk_a.unacked) == {1, 2}
    assert set(bk_b.unacked) == {1, 2}
    # the two tag-1 deliveries are distinct messages: no shared ownership
    ids_a = {id(m) for m in bk_a.unacked.values()}
    ids_b = {id(m) for m in bk_b.unacked.values()}
    assert ids_a.isdisjoint(ids_b)

    # ack tag 1 on A only: B's tag 1 must stay unacked and must NOT be
    # redelivered anywhere
    rec_a.ack(0)
    wait_for(lambda: set(bk_a.unacked) == {2})
    assert set(bk_b.unacked) == {1, 2}
    assert len(server.broker.messages) == 0

    # B's tag 1 is still a valid credential on B
    rec_b.ack(0)
    wait_for(lambda: set(bk_b.unacked) == {2})
    assert bk_b.is_open and ch_b.is_open

    rec_a.ack(1)
    rec_b.ack(1)
    wait_for(lambda: not bk_a.unacked and not bk_b.unacked)
    assert len(server.broker.messages) == 0

    got_a = [e.body for e in rec_a.events]
    got_b = [e.body for e in rec_b.events]
    assert sorted(got_a + got_b) == sorted(bodies)
    assert not set(got_a) & set(got_b)  # nothing delivered to both consumers
    assert all(not e.redelivered for e in rec_a.events + rec_b.events)
    conn_a.close()
    conn_b.close()


def test_multiple_ack_is_cumulative_within_channel(server):
    conn = pika_connect(server.port)
    ch = conn.channel()
    ch.queue_declare(queue="demo")
    ch.basic_qos(prefetch_count=2)
    rec = Recorder(ch)
    pub = conn.channel()

    bodies = [b"m%d" % i for i in range(4)]
    for body in bodies:
        pub.basic_publish(exchange="", routing_key="demo", body=body)

    pump_until(conn, lambda: len(rec.events) == 2)
    assert [e.tag for e in rec.events] == [1, 2]
    bk_ch = broker_channels(server)[0][1]
    assert set(bk_ch.unacked) == {1, 2}

    rec.ack(tag=2, multiple=True)  # settles 1 and 2 on this channel
    pump_until(conn, lambda: len(rec.events) == 4)  # freed credit delivers m2, m3
    assert [e.tag for e in rec.events] == [1, 2, 3, 4]
    assert [e.body for e in rec.events] == bodies
    wait_for(lambda: set(bk_ch.unacked) == {3, 4})  # tags 1,2 settled; 3,4 outstanding

    rec.ack(tag=4, multiple=True)
    wait_for(lambda: not bk_ch.unacked)
    assert len(server.broker.messages) == 0
    conn.close()


def test_unknown_tag_closes_only_offending_channel(server):
    conn_a = pika_connect(server.port)
    conn_b = pika_connect(server.port)
    ch_a = conn_a.channel()
    ch_b = conn_b.channel()
    ch_a.queue_declare(queue="demo")
    ch_b.queue_declare(queue="demo")
    ch_a.basic_qos(prefetch_count=2)
    ch_b.basic_qos(prefetch_count=2)
    rec_a = Recorder(ch_a)
    rec_b = Recorder(ch_b)

    pub = conn_a.channel()
    pub.basic_publish(exchange="", routing_key="demo", body=b"m0")
    pub.basic_publish(exchange="", routing_key="demo", body=b"m1")
    pump_until([conn_a, conn_b], lambda: len(rec_a.events) == 1 and len(rec_b.events) == 1)

    # A acks a tag it never received: only A's channel dies, its unacked
    # message is requeued and redelivered to B
    ch_a.basic_ack(delivery_tag=999)
    pump_until(conn_a, lambda: ch_a.is_closed)

    pump_until(conn_b, lambda: len(rec_b.events) == 2)
    assert rec_b.events[1].body == b"m0"
    assert rec_b.events[1].redelivered is True

    # B and the rest of connection A are unaffected
    rec_b.ack(0)
    rec_b.ack(1)
    pub.basic_publish(exchange="", routing_key="demo", body=b"m2")
    pump_until(conn_b, lambda: len(rec_b.events) == 3)
    assert rec_b.events[2].body == b"m2"
    assert rec_b.events[2].redelivered is False
    rec_b.ack(2)
    wait_for(lambda: len(server.broker.messages) == 0)
    conn_a.close()
    conn_b.close()


def test_duplicate_ack_closes_channel_and_requeues_remainder(server):
    conn_a = pika_connect(server.port)
    conn_b = pika_connect(server.port)
    ch_a = conn_a.channel()
    ch_b = conn_b.channel()
    ch_a.queue_declare(queue="demo")
    ch_b.queue_declare(queue="demo")
    ch_a.basic_qos(prefetch_count=2)
    ch_b.basic_qos(prefetch_count=2)
    rec_a = Recorder(ch_a)
    rec_b = Recorder(ch_b)

    pub = conn_a.channel()
    for body in (b"m0", b"m1", b"m2"):
        pub.basic_publish(exchange="", routing_key="demo", body=body)
    # A holds m0(tag1)+m2(tag2), B holds m1(tag1)
    pump_until([conn_a, conn_b], lambda: len(rec_a.events) == 2 and len(rec_b.events) == 1)

    rec_a.ack(0)  # valid: settles m0
    bk_a = broker_channels(server)[0][1]
    wait_for(lambda: set(bk_a.unacked) == {2})
    ch_a.basic_ack(delivery_tag=1)  # duplicate: tag 1 already settled
    pump_until(conn_a, lambda: ch_a.is_closed)

    # A's remaining unacked message (m2) is returned and redelivered to B
    pump_until(conn_b, lambda: len(rec_b.events) == 2)
    assert rec_b.events[1].body == b"m2"
    assert rec_b.events[1].redelivered is True
    rec_b.ack(0)
    rec_b.ack(1)
    wait_for(lambda: len(server.broker.messages) == 0)
    conn_a.close()
    conn_b.close()


def test_graceful_disconnect_returns_unacked_in_order(server):
    conn_a = pika_connect(server.port)
    ch_a = conn_a.channel()
    ch_a.queue_declare(queue="demo")
    ch_a.basic_qos(prefetch_count=2)
    rec_a = Recorder(ch_a)
    pub = conn_a.channel()
    bodies = [b"m%d" % i for i in range(4)]
    for body in bodies:
        pub.basic_publish(exchange="", routing_key="demo", body=body)
    pump_until(conn_a, lambda: len(rec_a.events) == 2)  # m0, m1 unacked on A

    conn_a.close()  # m0, m1 must be returned ahead of m2, m3
    wait_for(lambda: len(server.broker.connections) == 0)
    wait_for(lambda: len(server.broker.messages) == 4)

    conn_b = pika_connect(server.port)
    ch_b = conn_b.channel()
    ch_b.queue_declare(queue="demo")
    ch_b.basic_qos(prefetch_count=2)
    rec_b = Recorder(ch_b)
    pump_until(conn_b, lambda: len(rec_b.events) == 2)
    assert [e.body for e in rec_b.events] == bodies[:2]
    assert [e.redelivered for e in rec_b.events] == [True, True]
    rec_b.ack(tag=2, multiple=True)
    pump_until(conn_b, lambda: len(rec_b.events) == 4)
    assert [e.body for e in rec_b.events] == bodies
    assert [e.redelivered for e in rec_b.events[2:]] == [False, False]
    rec_b.ack(tag=4, multiple=True)
    wait_for(lambda: len(server.broker.messages) == 0)
    conn_b.close()


def test_prefetch_one_credit_and_nack_semantics(server):
    conn = pika_connect(server.port)
    ch = conn.channel()
    ch.queue_declare(queue="demo")
    ch.basic_qos(prefetch_count=1)
    rec = Recorder(ch)
    pub = conn.channel()
    for body in (b"m0", b"m1", b"m2"):
        pub.basic_publish(exchange="", routing_key="demo", body=body)

    pump_until(conn, lambda: len(rec.events) == 1)
    # credit is held until ack/nack: nothing more arrives
    with pytest.raises(AssertionError):
        pump_until(conn, lambda: len(rec.events) == 2, timeout=0.5)

    rec.nack(0, requeue=True)  # returns m0, credit released
    pump_until(conn, lambda: len(rec.events) == 2)
    assert rec.events[1].body == b"m0"
    assert rec.events[1].redelivered is True

    rec.nack(1, requeue=False)  # drops m0 for good
    pump_until(conn, lambda: len(rec.events) == 3)
    assert rec.events[2].body == b"m1"
    assert rec.events[2].redelivered is False

    rec.ack(2)
    pump_until(conn, lambda: len(rec.events) == 4)
    assert rec.events[3].body == b"m2"
    rec.ack(3)
    wait_for(lambda: len(server.broker.messages) == 0)
    conn.close()


def test_queue_capacity_is_twenty(server):
    conn = pika_connect(server.port)
    ch = conn.channel()
    ch.queue_declare(queue="demo")
    for i in range(25):
        ch.basic_publish(exchange="", routing_key="demo", body=b"x%02d" % i)
    wait_for(lambda: len(server.broker.messages) == 20)
    time.sleep(0.3)
    assert len(server.broker.messages) == 20  # 5 dropped

    ch.basic_qos(prefetch_count=2)
    rec = Recorder(ch)
    received = 0
    while received < 20:
        pump_until(conn, lambda: len(rec.events) > received)
        rec.ack(received)
        received += 1
    wait_for(lambda: len(server.broker.messages) == 0)
    conn.close()


def test_at_most_two_connections(server):
    conn_a = pika_connect(server.port)
    conn_b = pika_connect(server.port)
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        pika_connect(server.port)
    conn_a.close()
    wait_for(lambda: len(server.broker.connections) == 1)
    conn_c = pika_connect(server.port)  # slot freed: works again
    conn_b.close()
    conn_c.close()


def test_at_most_two_channels_per_connection(server):
    conn = pika_connect(server.port)
    ch1 = conn.channel()
    ch2 = conn.channel()
    # pika honours the negotiated channel-max=2 locally...
    with pytest.raises(pika.exceptions.NoFreeChannels):
        conn.channel()
    # ...and the connection stays fully usable
    assert ch1.queue_declare(queue="demo").method.message_count == 0
    assert ch2.queue_declare(queue="demo").method.message_count == 0
    conn.close()


def test_message_never_owned_by_two_consumers(server):
    conn_a = pika_connect(server.port)
    conn_b = pika_connect(server.port)
    ch_a = conn_a.channel()
    ch_b = conn_b.channel()
    ch_a.queue_declare(queue="demo")
    ch_b.queue_declare(queue="demo")
    ch_a.basic_qos(prefetch_count=1)
    ch_b.basic_qos(prefetch_count=1)
    rec_a = Recorder(ch_a)
    rec_b = Recorder(ch_b)

    pub = conn_a.channel()
    bodies = [b"uniq-%d" % i for i in range(8)]
    for body in bodies:
        pub.basic_publish(exchange="", routing_key="demo", body=body)

    # each side acks whatever it gets until the queue is drained
    seen_a, seen_b = [], []
    acked_a = acked_b = 0
    deadline = time.monotonic() + 5.0
    while len(seen_a) + len(seen_b) < 8 and time.monotonic() < deadline:
        for conn in (conn_a, conn_b):
            conn.process_data_events(time_limit=0.02)
        while acked_a < len(rec_a.events):
            seen_a.append(rec_a.events[acked_a].body)
            rec_a.ack(acked_a)
            acked_a += 1
        while acked_b < len(rec_b.events):
            seen_b.append(rec_b.events[acked_b].body)
            rec_b.ack(acked_b)
            acked_b += 1

    assert sorted(seen_a + seen_b) == sorted(bodies)  # nothing lost, nothing duplicated
    assert not set(seen_a) & set(seen_b)  # never owned by both consumers
    wait_for(lambda: len(server.broker.messages) == 0)
    channels = broker_channels(server)
    wait_for(lambda: not channels[0][1].unacked and not channels[1][1].unacked)
    conn_a.close()
    conn_b.close()


def test_normal_close_and_reconnect(server):
    conn = pika_connect(server.port)
    ch = conn.channel()
    ch.queue_declare(queue="demo")
    ch.close()
    conn.close()
    wait_for(lambda: len(server.broker.connections) == 0)

    conn2 = pika_connect(server.port)
    ch2 = conn2.channel()
    assert ch2.queue_declare(queue="demo").method.message_count == 0
    conn2.close()
