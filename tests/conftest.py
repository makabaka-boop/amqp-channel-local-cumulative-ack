"""Shared pytest fixtures and helpers.

The broker runs on a background thread with its own asyncio loop; tests
drive it with real pika clients (test_broker.py) or raw sockets
(test_frames.py) from the main thread.
"""

import asyncio
import os
import socket
import struct
import sys
import threading
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker import Broker  # noqa: E402


class BrokerHandle:
    """A Broker running on a private event loop in a daemon thread."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.broker = None

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def start(self, **kwargs):
        self.broker = Broker(host="127.0.0.1", port=0, **kwargs)
        self.thread.start()
        asyncio.run_coroutine_threadsafe(self.broker.start(), self.loop).result(timeout=5)
        return self

    def stop(self):
        if self.broker is not None:
            asyncio.run_coroutine_threadsafe(self.broker.stop(), self.loop).result(timeout=5)
            asyncio.run_coroutine_threadsafe(asyncio.sleep(0.05), self.loop).result(timeout=5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)

    @property
    def port(self):
        return self.broker.port


@pytest.fixture()
def server():
    handle = BrokerHandle().start()
    yield handle
    handle.stop()


def wait_for(cond, timeout=5.0, interval=0.01, msg="condition not met"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    raise AssertionError(msg)


# ---------------------------------------------------------------------------
# pika helpers
# ---------------------------------------------------------------------------

def pika_connect(port):
    import pika

    params = pika.ConnectionParameters(
        host="127.0.0.1",
        port=port,
        heartbeat=0,
        blocked_connection_timeout=5,
        connection_attempts=1,
        retry_delay=0,
        socket_timeout=5,
    )
    return pika.BlockingConnection(params)


def pump_until(connections, cond, timeout=5.0):
    """Pump pika i/o on the given connections until cond() is true."""
    if not isinstance(connections, (list, tuple)):
        connections = [connections]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for conn in connections:
            if conn.is_open:
                conn.process_data_events(time_limit=0.02)
        if cond():
            return True
        time.sleep(0.005)
    raise AssertionError("pump_until: condition not met within timeout")


class Recorder:
    """basic_consume wrapper recording every delivery."""

    def __init__(self, channel, queue="demo"):
        self.channel = channel
        self.events = []
        channel.basic_consume(queue=queue, on_message_callback=self._on, auto_ack=False)

    def _on(self, _channel, method, properties, body):
        self.events.append(
            SimpleNamespace(
                tag=method.delivery_tag,
                redelivered=method.redelivered,
                body=body,
                properties=properties,
            )
        )

    def ack(self, index=None, tag=None, multiple=False):
        delivery_tag = tag if tag is not None else self.events[index].tag
        self.channel.basic_ack(delivery_tag=delivery_tag, multiple=multiple)

    def nack(self, index=None, tag=None, multiple=False, requeue=True):
        delivery_tag = tag if tag is not None else self.events[index].tag
        self.channel.basic_nack(delivery_tag=delivery_tag, multiple=multiple, requeue=requeue)


# ---------------------------------------------------------------------------
# raw socket AMQP client (frame-level tests)
# ---------------------------------------------------------------------------

class RawClient:
    """Minimal hand-rolled AMQP client giving full control over framing."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.buf = bytearray()

    # -- low level ------------------------------------------------------

    def write(self, data: bytes):
        self.sock.sendall(data)

    def write_frame(self, frame_obj, channel=0):
        from pamqp.frame import marshal

        self.write(marshal(frame_obj, channel))

    def write_frame_fragmented(self, frame_obj, channel=0, chunk=3, delay=0.0):
        from pamqp.frame import marshal

        data = marshal(frame_obj, channel)
        for i in range(0, len(data), chunk):
            self.write(data[i : i + chunk])
            if delay:
                time.sleep(delay)

    def read_frame(self, timeout=5.0):
        from pamqp.frame import unmarshal

        deadline = time.monotonic() + timeout
        while True:
            if len(self.buf) >= 8:
                size = struct.unpack(">I", bytes(self.buf[3:7]))[0]
                total = 8 + size
                if len(self.buf) >= total:
                    raw = bytes(self.buf[:total])
                    del self.buf[:total]
                    _, channel, obj = unmarshal(raw)
                    return channel, obj
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for a frame")
            self.sock.settimeout(remaining)
            data = self.sock.recv(65536)
            if not data:
                raise EOFError("broker closed the connection")
            self.buf += data

    def expect_no_frame(self, seconds=0.3):
        try:
            channel, obj = self.read_frame(timeout=seconds)
        except (TimeoutError, socket.timeout):
            return
        raise AssertionError(f"unexpected frame on channel {channel}: {obj!r}")

    # -- protocol ---------------------------------------------------------

    def handshake(self, fragment_header=False):
        from pamqp import commands as spec

        header = b"AMQP\x00\x00\x09\x01"
        if fragment_header:
            self.write(header[:4])
            time.sleep(0.05)
            self.write(header[4:])
        else:
            self.write(header)
        _, start = self.read_frame()
        assert isinstance(start, spec.Connection.Start)
        self.write_frame(
            spec.Connection.StartOk(
                client_properties={"product": "raw-test"},
                mechanism="PLAIN",
                response="\x00guest\x00guest",
                locale="en_US",
            )
        )
        _, tune = self.read_frame()
        assert isinstance(tune, spec.Connection.Tune)
        self.write_frame(
            spec.Connection.TuneOk(
                channel_max=tune.channel_max, frame_max=tune.frame_max, heartbeat=0
            )
        )
        self.write_frame(spec.Connection.Open(virtual_host="/"))
        _, open_ok = self.read_frame()
        assert isinstance(open_ok, spec.Connection.OpenOk)

    def open_channel(self, channel_id):
        from pamqp import commands as spec

        self.write_frame(spec.Channel.Open(), channel_id)
        channel, obj = self.read_frame()
        assert channel == channel_id and isinstance(obj, spec.Channel.OpenOk), obj

    def declare(self, channel_id, queue="demo"):
        from pamqp import commands as spec

        self.write_frame(spec.Queue.Declare(queue=queue), channel_id)
        channel, obj = self.read_frame()
        assert channel == channel_id and isinstance(obj, spec.Queue.DeclareOk), obj
        return obj

    def qos(self, channel_id, prefetch_count):
        from pamqp import commands as spec

        self.write_frame(
            spec.Basic.Qos(prefetch_size=0, prefetch_count=prefetch_count, global_=False),
            channel_id,
        )
        channel, obj = self.read_frame()
        assert isinstance(obj, spec.Basic.QosOk), obj

    def consume(self, channel_id, queue="demo", consumer_tag=""):
        from pamqp import commands as spec

        self.write_frame(
            spec.Basic.Consume(queue=queue, consumer_tag=consumer_tag, no_ack=False),
            channel_id,
        )
        channel, obj = self.read_frame()
        assert isinstance(obj, spec.Basic.ConsumeOk), obj
        return obj.consumer_tag

    def publish(self, channel_id, body, routing_key="demo", exchange="",
                body_chunk=None, coalesce=False):
        """Send a complete publish; optionally split the body into frames and
        optionally coalesce everything into a single TCP write."""
        from pamqp import commands as spec
        from pamqp.body import ContentBody
        from pamqp.frame import marshal
        from pamqp.header import ContentHeader

        frames = [
            marshal(spec.Basic.Publish(exchange=exchange, routing_key=routing_key), channel_id),
            marshal(
                ContentHeader(body_size=len(body), properties=spec.Basic.Properties()),
                channel_id,
            ),
        ]
        if body:
            if body_chunk:
                for i in range(0, len(body), body_chunk):
                    frames.append(marshal(ContentBody(body[i : i + body_chunk]), channel_id))
            else:
                frames.append(marshal(ContentBody(body), channel_id))
        if coalesce:
            self.write(b"".join(frames))
        else:
            for frame in frames:
                self.write(frame)

    def read_delivery(self, timeout=5.0):
        """Read one delivery; returns (channel, deliver_method, body)."""
        from pamqp.body import ContentBody
        from pamqp.header import ContentHeader
        from pamqp import commands as spec

        channel, deliver = self.read_frame(timeout)
        assert isinstance(deliver, spec.Basic.Deliver), deliver
        _, header = self.read_frame(timeout)
        assert isinstance(header, ContentHeader), header
        body = b""
        while len(body) < header.body_size:
            _, chunk = self.read_frame(timeout)
            assert isinstance(chunk, ContentBody), chunk
            body += chunk.value
        return channel, deliver, body

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass
