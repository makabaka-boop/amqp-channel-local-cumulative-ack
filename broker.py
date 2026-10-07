#!/usr/bin/env python3
"""In-memory, single-queue AMQP 0-9-1 demo broker.

Scope (demo only):

* at most 2 connections, at most 2 channels per connection;
* one fixed queue (``demo``) holding at most 20 messages of at most
  256 bytes each;
* connection/channel handshakes, fixed ``queue.declare``, default-exchange
  ``basic.publish``, ``basic.consume`` with manual acknowledgements,
  ``basic.qos`` (prefetch 1-2 supported), ``basic.ack`` / ``basic.nack`` /
  ``basic.reject``, ``basic.cancel`` and graceful channel/connection close;
* no persistence, no publisher confirms, no transactions, no heartbeats.

pamqp is used purely for frame/method (un)marshalling.  Queueing,
acknowledgement bookkeeping and delivery are managed here.

Invariants implemented in this file:

* delivery-tags are scoped to a channel and increase monotonically per
  channel; two channels may legitimately hand out tag ``1`` at the same
  time.  Unacked state is therefore keyed *per channel*, never globally,
  so an ack on one channel can never settle another channel's delivery.
* a publish (method + content header + body fragments) is assembled per
  channel and enqueued only once complete; frames from other channels may
  interleave freely.  Incomplete (connection lost mid-publish) or
  oversized (> 256 byte) bodies never become messages.
* an ack/nack for an unknown or already-acked (duplicate) tag closes only
  the offending channel; that channel's unacked messages are requeued
  while every other channel keeps working.
* requeued messages keep their original enqueue sequence, are delivered
  before younger messages and are marked ``redelivered``.
* prefetch credit is released only when a delivery is acked or returned
  (nack/requeue, channel or connection close).
* delivery is dispatched synchronously inside the event loop and every
  candidate channel is re-checked for liveness right before its frames are
  written, so a consumer that has since closed never receives a delivery.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import logging
import struct
from typing import Optional

from pamqp import commands as spec
from pamqp.body import ContentBody
from pamqp.exceptions import UnmarshalingException
from pamqp.frame import marshal as marshal_frame
from pamqp.frame import unmarshal as unmarshal_frame
from pamqp.header import ContentHeader
from pamqp.heartbeat import Heartbeat

LOGGER = logging.getLogger("amqp-broker")

PROTOCOL_HEADER = b"AMQP\x00\x00\x09\x01"
FRAME_END = 0xCE
_FRAME_HEADER = struct.Struct(">BHI")  # frame-type, channel, payload-size

# AMQP 0-9-1 reply codes
NOT_FOUND = 404
PRECONDITION_FAILED = 406
FRAME_ERROR = 501
UNEXPECTED_FRAME = 505
NOT_ALLOWED = 530
NOT_IMPLEMENTED = 540
CHANNEL_ERROR = 504

DEFAULT_QUEUE = "demo"
MAX_MESSAGES = 20
MAX_BODY_SIZE = 256
MAX_CONNECTIONS = 2
MAX_CHANNELS = 2
FRAME_MAX = 131072
READ_CHUNK = 65536


def _method_ids(frame_obj) -> tuple[int, int]:
    """(class-id, method-id) of a method frame, for error reporting."""
    index = getattr(frame_obj, "index", 0) or 0
    return index >> 16, index & 0xFFFF


class Message:
    """A queued message; ``seq`` is its broker-wide first-enqueue sequence."""

    __slots__ = ("seq", "body", "properties", "redelivered")

    def __init__(self, seq: int, body: bytes, properties) -> None:
        self.seq = seq
        self.body = body
        self.properties = properties
        self.redelivered = False

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Message(seq={self.seq}, {len(self.body)}B, redelivered={self.redelivered})"


class Broker:
    """Owns the single queue, the consumer registry and the dispatch loop."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 5672,
        queue_name: str = DEFAULT_QUEUE,
        max_messages: int = MAX_MESSAGES,
        max_body_size: int = MAX_BODY_SIZE,
        max_connections: int = MAX_CONNECTIONS,
        max_channels: int = MAX_CHANNELS,
    ) -> None:
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.max_messages = max_messages
        self.max_body_size = max_body_size
        self.max_connections = max_connections
        self.max_channels = max_channels

        self.messages: list[Message] = []  # ready messages, ordered by seq
        self.consumers: list[Channel] = []  # channels with an active consumer
        self.connections: set[Connection] = set()
        self._rr_index = 0  # round-robin cursor into self.consumers
        self._seq = 0
        self.server: Optional[asyncio.AbstractServer] = None

    # -- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._accept, self.host, self.port)
        if self.port == 0:
            self.port = self.server.sockets[0].getsockname()[1]
        LOGGER.info("listening on %s:%d queue=%r", self.host, self.port, self.queue_name)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for conn in list(self.connections):
            conn.abort()

    async def _accept(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        if len(self.connections) >= self.max_connections:
            LOGGER.warning("rejecting %s: connection limit %d", peer, self.max_connections)
            writer.close()
            return
        conn = Connection(self, reader, writer)
        self.connections.add(conn)
        LOGGER.info(
            "connection %d from %s (%d/%d)",
            conn.ident,
            peer,
            len(self.connections),
            self.max_connections,
        )
        await conn.run()

    # -- queue operations -------------------------------------------------

    def enqueue(self, body: bytes, properties) -> bool:
        """Append a fully-assembled message; returns False (drops) when full."""
        if len(self.messages) >= self.max_messages:
            LOGGER.warning(
                "queue full (%d), dropping %d-byte message", len(self.messages), len(body)
            )
            return False
        self._seq += 1
        self.messages.append(Message(self._seq, body, properties))
        self._dispatch_ready()
        return True

    def requeue(self, messages) -> None:
        """Return messages to the queue, ordered by first-enqueue sequence."""
        msgs = list(messages)
        for msg in msgs:
            msg.redelivered = True
            bisect.insort(self.messages, msg, key=lambda m: m.seq)
        if msgs:
            LOGGER.info("requeued seqs=%s", [m.seq for m in msgs])
            self._dispatch_ready()

    # -- consumer registry --------------------------------------------------

    def add_consumer(self, channel: "Channel") -> None:
        self.consumers.append(channel)
        self._dispatch_ready()

    def remove_consumer(self, channel: "Channel") -> None:
        if channel in self.consumers:
            self.consumers.remove(channel)

    # -- delivery -----------------------------------------------------------

    def _dispatch_ready(self) -> None:
        """Move ready messages to consumers with available credit.

        Runs synchronously inside the event loop; the chosen channel is
        re-validated by :meth:`Channel.deliver` immediately before its frames
        are written, so a consumer that has since closed never gets a
        delivery from a stale dispatch.
        """
        while self.messages:
            channel = self._next_consumer()
            if channel is None:
                return
            msg = self.messages.pop(0)
            channel.deliver(msg)

    def _next_consumer(self) -> Optional["Channel"]:
        count = len(self.consumers)
        for offset in range(count):
            idx = (self._rr_index + offset) % count
            channel = self.consumers[idx]
            if channel.has_credit():
                self._rr_index = (idx + 1) % count
                return channel
        return None


class Connection:
    """One TCP connection: framing, handshake, channel demux."""

    _ids = 0

    def __init__(
        self,
        broker: Broker,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.broker = broker
        self.reader = reader
        self.writer = writer
        Connection._ids += 1
        self.ident = Connection._ids
        self.channels: dict[int, Channel] = {}
        self.buffer = bytearray()
        self.got_protocol_header = False
        self.alive = True
        self._finalized = False

    # -- socket loop --------------------------------------------------------

    async def run(self) -> None:
        try:
            while True:
                data = await self.reader.read(READ_CHUNK)
                if not data:
                    break
                self.buffer += data
                self._process_buffer()
                try:
                    await self.writer.drain()
                except (ConnectionError, RuntimeError):
                    break
                if not self.alive:
                    # Our Close (or CloseOk) has been flushed; stop reading.
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception:  # pragma: no cover - defensive
            LOGGER.exception("connection %d failed", self.ident)
        finally:
            self._finalize()

    def abort(self) -> None:
        self.alive = False
        try:
            self.writer.close()
        except Exception:  # pragma: no cover - defensive
            pass

    def _finalize(self) -> None:
        """Connection is gone: return every channel's unacked messages."""
        if self._finalized:
            return
        self._finalized = True
        self.alive = False
        for channel in list(self.channels.values()):
            channel.finalize()
        self.broker.connections.discard(self)
        try:
            self.writer.close()
        except Exception:  # pragma: no cover - defensive
            pass
        LOGGER.info("connection %d closed", self.ident)

    # -- outgoing -----------------------------------------------------------

    def send(self, frame_obj, channel_id: int = 0) -> None:
        if not self.alive:
            return
        try:
            self.writer.write(marshal_frame(frame_obj, channel_id))
        except (ConnectionError, RuntimeError):
            self.alive = False

    # -- incoming -----------------------------------------------------------

    def _process_buffer(self) -> None:
        buf = self.buffer
        if not self.got_protocol_header:
            if len(buf) < len(PROTOCOL_HEADER):
                return
            if bytes(buf[: len(PROTOCOL_HEADER)]) != PROTOCOL_HEADER:
                LOGGER.warning("connection %d: bad protocol header", self.ident)
                self.writer.write(PROTOCOL_HEADER)  # AMQP-mandated response
                self.alive = False
                return
            del buf[: len(PROTOCOL_HEADER)]
            self.got_protocol_header = True
            self.send(
                spec.Connection.Start(
                    version_major=0,
                    version_minor=9,
                    server_properties={
                        "product": "in-memory-demo-broker",
                        "version": "1.0.0",
                        "capabilities": {},
                    },
                    mechanisms="PLAIN",
                    locales="en_US",
                )
            )

        while self.alive:
            if len(buf) < _FRAME_HEADER.size:
                return
            _, _, size = _FRAME_HEADER.unpack(bytes(buf[: _FRAME_HEADER.size]))
            if size > FRAME_MAX:
                self._connection_error(FRAME_ERROR, "frame exceeds frame_max")
                return
            total = _FRAME_HEADER.size + size + 1
            if len(buf) < total:
                return  # incomplete frame: wait for more TCP data
            if buf[total - 1] != FRAME_END:
                self._connection_error(FRAME_ERROR, "invalid frame-end octet")
                return
            raw = bytes(buf[:total])
            del buf[:total]
            try:
                _, chan, frame_obj = unmarshal_frame(raw)
            except UnmarshalingException as exc:
                self._connection_error(FRAME_ERROR, f"cannot decode frame: {exc}")
                return
            self._dispatch(chan, frame_obj)

    def _dispatch(self, channel_id: int, frame_obj) -> None:
        if isinstance(frame_obj, Heartbeat):
            return
        if isinstance(frame_obj, (ContentHeader, ContentBody)):
            channel = self.channels.get(channel_id)
            if channel is None or not channel.is_open:
                self._connection_error(UNEXPECTED_FRAME, "content frame on non-open channel")
                return
            channel.on_content(frame_obj)
            return
        # method frames
        if channel_id == 0:
            self._on_connection_method(frame_obj)
            return
        if isinstance(frame_obj, spec.Channel.Open):
            self._on_channel_open(channel_id)
            return
        channel = self.channels.get(channel_id)
        if channel is None:
            self._connection_error(CHANNEL_ERROR, f"unknown channel {channel_id}")
            return
        channel.on_method(frame_obj)

    # -- connection-level methods -------------------------------------------

    def _on_connection_method(self, frame_obj) -> None:
        if isinstance(frame_obj, spec.Connection.StartOk):
            self.send(
                spec.Connection.Tune(
                    channel_max=self.broker.max_channels,
                    frame_max=FRAME_MAX,
                    heartbeat=0,
                )
            )
        elif isinstance(frame_obj, spec.Connection.TuneOk):
            pass  # fixed offer, client agreed
        elif isinstance(frame_obj, spec.Connection.Open):
            self.send(spec.Connection.OpenOk())
            LOGGER.info("connection %d: open vhost=%r", self.ident, frame_obj.virtual_host)
        elif isinstance(frame_obj, spec.Connection.Close):
            LOGGER.info(
                "connection %d: client close %s %r",
                self.ident,
                frame_obj.reply_code,
                frame_obj.reply_text,
            )
            self.send(spec.Connection.CloseOk())
            self.alive = False
        elif isinstance(frame_obj, spec.Connection.CloseOk):
            self.alive = False
        else:
            LOGGER.warning("connection %d: ignoring %s", self.ident, frame_obj.name)

    def _connection_error(self, code: int, text: str) -> None:
        LOGGER.warning("connection %d: error %s %s", self.ident, code, text)
        self.send(spec.Connection.Close(reply_code=code, reply_text=text, class_id=0, method_id=0))
        self.alive = False

    # -- channels -------------------------------------------------------------

    def _on_channel_open(self, channel_id: int) -> None:
        if channel_id < 1 or channel_id > self.broker.max_channels:
            self._connection_error(CHANNEL_ERROR, f"invalid channel id {channel_id}")
            return
        old = self.channels.get(channel_id)
        if old is not None and old.is_open:
            self._connection_error(CHANNEL_ERROR, f"channel {channel_id} already open")
            return
        if old is not None:
            old.finalize()
        self.channels[channel_id] = Channel(self, channel_id)
        self.send(spec.Channel.OpenOk(), channel_id)
        LOGGER.info("connection %d: channel %d open", self.ident, channel_id)


class Channel:
    """Per-channel state: publish assembly, consumer, unacked deliveries.

    ``unacked`` maps this channel's own delivery-tags to messages.  Tags are
    only meaningful within this channel, so nothing here is ever looked up
    globally.
    """

    def __init__(self, connection: Connection, channel_id: int) -> None:
        self.connection = connection
        self.broker = connection.broker
        self.id = channel_id
        self.state = "open"
        # publish assembly (per channel; other channels may interleave frames)
        self._pub_method: Optional[spec.Basic.Publish] = None
        self._pub_header: Optional[ContentHeader] = None
        self._pub_chunks: list[bytes] = []
        self._pub_received = 0
        # consume state
        self.consumer_tag: Optional[str] = None
        self.no_ack = False
        self.prefetch = 0  # 0 = unlimited; clients are expected to use 1-2
        self.unacked: dict[int, Message] = {}
        self.next_delivery_tag = 1
        self._finalized = False

    @property
    def is_open(self) -> bool:
        return self.state == "open"

    # -- delivery -------------------------------------------------------------

    def has_credit(self) -> bool:
        return (
            self.is_open
            and self.consumer_tag is not None
            and self.connection.alive
            and (self.no_ack or self.prefetch == 0 or len(self.unacked) < self.prefetch)
        )

    def deliver(self, msg: Message) -> None:
        """Send one message to this channel's consumer.

        Liveness is re-checked here so a dispatch that was decided before the
        channel closed can never put frames on the wire.
        """
        if not self.has_credit():
            self.broker.requeue([msg])  # defensive; unreachable in practice
            return
        tag = self.next_delivery_tag
        self.next_delivery_tag += 1
        if not self.no_ack:
            self.unacked[tag] = msg
        conn = self.connection
        conn.send(
            spec.Basic.Deliver(
                consumer_tag=self.consumer_tag,
                delivery_tag=tag,
                redelivered=msg.redelivered,
                exchange="",
                routing_key=self.broker.queue_name,
            ),
            self.id,
        )
        conn.send(ContentHeader(body_size=len(msg.body), properties=msg.properties), self.id)
        if msg.body:
            conn.send(ContentBody(msg.body), self.id)

    # -- teardown ---------------------------------------------------------------

    def finalize(self) -> None:
        """Close the channel and return its unacked messages to the queue.

        Only this channel is affected: its unacked messages are requeued in
        original-enqueue order and marked redelivered; every other channel's
        deliveries and unacked state are untouched.
        """
        if self._finalized:
            return
        self._finalized = True
        self.state = "closed"
        self._reset_publish()  # incomplete publish never becomes a message
        if self.consumer_tag is not None:
            self.broker.remove_consumer(self)
            self.consumer_tag = None
        if self.unacked:
            returned = list(self.unacked.values())
            self.unacked.clear()
            LOGGER.info(
                "connection %d channel %d: returning %d unacked message(s)",
                self.connection.ident,
                self.id,
                len(returned),
            )
            self.broker.requeue(returned)

    def _channel_error(self, code: int, text: str, frame_obj=None) -> None:
        class_id, method_id = _method_ids(frame_obj) if frame_obj is not None else (0, 0)
        LOGGER.warning(
            "connection %d channel %d: error %s %s",
            self.connection.ident,
            self.id,
            code,
            text,
        )
        self.connection.send(
            spec.Channel.Close(
                reply_code=code, reply_text=text, class_id=class_id, method_id=method_id
            ),
            self.id,
        )
        self.finalize()

    # -- method dispatch ---------------------------------------------------------

    def on_method(self, frame_obj) -> None:
        if not self.is_open:
            # Late frames for a closed channel: only the close handshake is
            # meaningful; everything else is dropped so a half-dead channel
            # can never ack, publish or consume again.
            if isinstance(frame_obj, spec.Channel.Close):
                self.connection.send(spec.Channel.CloseOk(), self.id)
            return
        handler = {
            spec.Channel.Close: self._on_close,
            spec.Channel.CloseOk: self._on_close_ok,
            spec.Channel.Flow: self._on_flow,
            spec.Queue.Declare: self._on_queue_declare,
            spec.Basic.Qos: self._on_qos,
            spec.Basic.Consume: self._on_consume,
            spec.Basic.Cancel: self._on_cancel,
            spec.Basic.Publish: self._on_publish,
            spec.Basic.Ack: self._on_ack,
            spec.Basic.Nack: self._on_nack,
            spec.Basic.Reject: self._on_reject,
        }.get(type(frame_obj))
        if handler is None:
            self._channel_error(NOT_IMPLEMENTED, f"{frame_obj.name} not implemented", frame_obj)
            return
        handler(frame_obj)

    def _on_close(self, frame_obj) -> None:
        self.connection.send(spec.Channel.CloseOk(), self.id)
        self.finalize()

    def _on_close_ok(self, frame_obj) -> None:
        self.finalize()

    def _on_flow(self, frame_obj) -> None:
        self.connection.send(spec.Channel.FlowOk(active=True), self.id)

    def _on_queue_declare(self, frame_obj) -> None:
        requested = frame_obj.queue or self.broker.queue_name
        if requested != self.broker.queue_name:
            self._channel_error(
                PRECONDITION_FAILED,
                f"only fixed queue {self.broker.queue_name!r} exists",
                frame_obj,
            )
            return
        self.connection.send(
            spec.Queue.DeclareOk(
                queue=self.broker.queue_name,
                message_count=len(self.broker.messages),
                consumer_count=len(self.broker.consumers),
            ),
            self.id,
        )

    def _on_qos(self, frame_obj) -> None:
        self.prefetch = frame_obj.prefetch_count or 0
        self.connection.send(spec.Basic.QosOk(), self.id)
        self.broker._dispatch_ready()  # credit may have increased

    def _on_consume(self, frame_obj) -> None:
        if (frame_obj.queue or self.broker.queue_name) != self.broker.queue_name:
            self._channel_error(NOT_FOUND, f"no queue {frame_obj.queue!r}", frame_obj)
            return
        if self.consumer_tag is not None:
            self._channel_error(NOT_ALLOWED, "channel already has a consumer", frame_obj)
            return
        self.consumer_tag = frame_obj.consumer_tag or f"ctag{self.connection.ident}.{self.id}"
        self.no_ack = bool(frame_obj.no_ack)
        self.connection.send(spec.Basic.ConsumeOk(consumer_tag=self.consumer_tag), self.id)
        self.broker.add_consumer(self)

    def _on_cancel(self, frame_obj) -> None:
        tag = frame_obj.consumer_tag
        if self.consumer_tag == tag:
            self.broker.remove_consumer(self)
            self.consumer_tag = None
        self.connection.send(spec.Basic.CancelOk(consumer_tag=tag), self.id)

    # -- publish assembly ------------------------------------------------------

    def _on_publish(self, frame_obj) -> None:
        if self._pub_method is not None:
            self.connection._connection_error(
                UNEXPECTED_FRAME, "basic.publish while content is pending"
            )
            return
        if frame_obj.exchange != "":
            self._channel_error(NOT_FOUND, f"no exchange {frame_obj.exchange!r}", frame_obj)
            return
        self._pub_method = frame_obj
        self._pub_header = None
        self._pub_chunks = []
        self._pub_received = 0

    def on_content(self, frame_obj) -> None:
        if isinstance(frame_obj, ContentHeader):
            if self._pub_method is None or self._pub_header is not None:
                self.connection._connection_error(UNEXPECTED_FRAME, "unexpected content header")
                return
            self._pub_header = frame_obj
            if frame_obj.body_size == 0:
                self._finish_publish()
            return
        # ContentBody
        if self._pub_header is None:
            self.connection._connection_error(UNEXPECTED_FRAME, "unexpected content body")
            return
        self._pub_received += len(frame_obj.value)
        if self._pub_received > self._pub_header.body_size:
            self.connection._connection_error(FRAME_ERROR, "content body larger than declared")
            return
        if self._pub_received <= self.broker.max_body_size:
            self._pub_chunks.append(frame_obj.value)  # only keep what could ever enqueue
        if self._pub_received == self._pub_header.body_size:
            self._finish_publish()

    def _finish_publish(self) -> None:
        method = self._pub_method
        declared = self._pub_header.body_size
        properties = self._pub_header.properties
        body = b"".join(self._pub_chunks)
        self._reset_publish()
        if declared > self.broker.max_body_size:
            LOGGER.warning(
                "connection %d channel %d: dropping %d-byte message (max %d)",
                self.connection.ident,
                self.id,
                declared,
                self.broker.max_body_size,
            )
            return
        if method.routing_key != self.broker.queue_name:
            LOGGER.info(
                "connection %d channel %d: dropping message for %r (no binding)",
                self.connection.ident,
                self.id,
                method.routing_key,
            )
            return
        self.broker.enqueue(body, properties)

    def _reset_publish(self) -> None:
        self._pub_method = None
        self._pub_header = None
        self._pub_chunks = []
        self._pub_received = 0

    # -- acknowledgements --------------------------------------------------------

    def _on_ack(self, frame_obj) -> None:
        tag = frame_obj.delivery_tag or 0
        if frame_obj.multiple and tag == 0:
            settled = list(self.unacked)
        elif tag not in self.unacked:
            # duplicate or unknown tag: close only this channel
            self._channel_error(PRECONDITION_FAILED, f"unknown delivery-tag {tag}", frame_obj)
            return
        elif frame_obj.multiple:
            settled = [t for t in self.unacked if t <= tag]
        else:
            settled = [tag]
        for t in settled:
            del self.unacked[t]
        self.broker._dispatch_ready()  # released credit may unblock deliveries

    def _on_nack(self, frame_obj) -> None:
        tag = frame_obj.delivery_tag or 0
        if frame_obj.multiple and tag == 0:
            tags = list(self.unacked)
        elif tag not in self.unacked:
            self._channel_error(PRECONDITION_FAILED, f"unknown delivery-tag {tag}", frame_obj)
            return
        elif frame_obj.multiple:
            tags = [t for t in self.unacked if t <= tag]
        else:
            tags = [tag]
        returned = [self.unacked.pop(t) for t in tags]
        if frame_obj.requeue:
            self.broker.requeue(returned)
        else:
            LOGGER.info(
                "connection %d channel %d: dropping %d nacked message(s)",
                self.connection.ident,
                self.id,
                len(returned),
            )
            self.broker._dispatch_ready()

    def _on_reject(self, frame_obj) -> None:
        tag = frame_obj.delivery_tag or 0
        if tag not in self.unacked:
            self._channel_error(PRECONDITION_FAILED, f"unknown delivery-tag {tag}", frame_obj)
            return
        msg = self.unacked.pop(tag)
        if frame_obj.requeue:
            self.broker.requeue([msg])
        else:
            self.broker._dispatch_ready()


async def _serve(args: argparse.Namespace) -> None:
    broker = Broker(host=args.host, port=args.port)
    await broker.start()
    try:
        await asyncio.Event().wait()  # run forever
    finally:
        await broker.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5672)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_serve(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
