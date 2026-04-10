"""Tests for agent-message-bus."""

import pytest

from agent_message_bus import DeliveryStatus, Message, MessageBus, MessagePriority, MessageRouter

# --- Message Tests ---


class TestMessage:
    def test_message_creation(self):
        msg = Message(
            from_agent="agent_1",
            to_agent="agent_2",
            content={"key": "value"},
        )
        assert msg.from_agent == "agent_1"
        assert msg.to_agent == "agent_2"
        assert msg.content == {"key": "value"}
        assert msg.message_id is not None
        assert msg.delivery_status == DeliveryStatus.PENDING

    def test_message_with_ttl(self):
        msg = Message(
            from_agent="a",
            to_agent="b",
            content={},
            ttl=1.0,  # 1 second
        )
        assert msg.expires_at is not None
        assert not msg.is_expired()

    def test_message_expiration(self):
        import time

        msg = Message(
            from_agent="a",
            to_agent="b",
            content={},
            ttl=0.001,  # 1ms - effectively expired
        )
        time.sleep(0.01)  # wait a bit
        assert msg.is_expired()

    def test_message_no_expiration_when_ttl_zero(self):
        msg = Message(from_agent="a", to_agent="b", content={}, ttl=0.0)
        assert msg.expires_at is None
        assert not msg.is_expired()

    def test_message_broadcast(self):
        msg = Message(from_agent="a", to_agent="broadcast", content={})
        assert msg.is_broadcast()

    def test_message_serialization(self):
        msg = Message(
            from_agent="agent_1",
            to_agent="agent_2",
            content={"task": "compute"},
            priority=MessagePriority.P1_HIGH.value,
        )
        data = msg.to_dict()
        restored = Message.from_dict(data)

        assert restored.from_agent == msg.from_agent
        assert restored.to_agent == msg.to_agent
        assert restored.content == msg.content
        assert restored.priority == msg.priority
        assert restored.delivery_status == msg.delivery_status

    def test_message_priority_ordering(self):
        high = Message(
            from_agent="a", to_agent="b", content={},
            priority=MessagePriority.P0_CRITICAL.value,
        )
        low = Message(
            from_agent="a", to_agent="b", content={},
            priority=MessagePriority.P3_LOW.value,
        )
        assert high < low  # lower value = higher priority

    def test_message_priority_same_priority_earlier_first(self):
        import time
        p2 = MessagePriority.P2_NORMAL.value
        msg1 = Message(from_agent="a", to_agent="b", content={}, priority=p2)
        time.sleep(0.001)
        msg2 = Message(from_agent="a", to_agent="b", content={}, priority=p2)
        assert msg1 < msg2


# --- Router Tests ---


@pytest.mark.asyncio
class TestMessageRouter:
    async def test_route_unicast(self):
        router = MessageRouter()
        msg = Message(from_agent="a", to_agent="b", content={})
        targets = await router.route_message(msg, ["a", "b", "c"])
        assert targets == ["b"]

    async def test_route_unicast_unregistered_target(self):
        router = MessageRouter()
        msg = Message(from_agent="a", to_agent="x", content={})
        targets = await router.route_message(msg, ["a", "b", "c"])
        assert targets == []

    async def test_route_broadcast(self):
        router = MessageRouter()
        msg = Message(from_agent="a", to_agent="broadcast", content={})
        targets = await router.route_message(msg, ["a", "b", "c"])
        assert sorted(targets) == ["b", "c"]

    async def test_enqueue_and_dequeue(self):
        router = MessageRouter()
        msg = Message(from_agent="a", to_agent="b", content={"task": "x"})
        result = await router.enqueue_for_agent("b", msg)
        assert result is True

        retrieved = await router.get_next_for_agent("b")
        assert retrieved is not None
        assert retrieved.content == {"task": "x"}

    async def test_expired_message_skipped(self):
        router = MessageRouter()
        msg = Message(from_agent="a", to_agent="b", content={}, ttl=0.001)
        import time
        time.sleep(0.01)

        result = await router.enqueue_for_agent("b", msg)
        assert result is False  # expired messages rejected

    async def test_priority_ordering(self):
        router = MessageRouter()
        p3 = MessagePriority.P3_LOW.value
        p0 = MessagePriority.P0_CRITICAL.value
        low = Message(from_agent="a", to_agent="b", content={"p": "low"}, priority=p3)
        high = Message(from_agent="a", to_agent="b", content={"p": "high"}, priority=p0)

        await router.enqueue_for_agent("b", low)
        await router.enqueue_for_agent("b", high)

        first = await router.get_next_for_agent("b")
        assert first.content == {"p": "high"}

    async def test_queue_depth(self):
        router = MessageRouter()
        await router.enqueue_for_agent("b", Message(from_agent="a", to_agent="b", content={}))
        await router.enqueue_for_agent("b", Message(from_agent="a", to_agent="b", content={}))
        assert await router.get_queue_depth("b") == 2

    async def test_clear_queue(self):
        router = MessageRouter()
        await router.enqueue_for_agent("b", Message(from_agent="a", to_agent="b", content={}))
        msgs = await router.clear_agent_queue("b")
        assert len(msgs) == 1
        assert await router.get_queue_depth("b") == 0


# --- MessageBus Tests ---


@pytest.mark.asyncio
class TestMessageBus:
    async def test_register_and_unregister(self):
        bus = MessageBus()
        bus.register_agent("agent_1")
        assert bus.is_registered("agent_1")

        bus.unregister_agent("agent_1")
        assert not bus.is_registered("agent_1")

    async def test_point_to_point(self):
        bus = MessageBus()
        bus.register_agent("agent_1")
        bus.register_agent("agent_2")

        msg_id = await bus.send("agent_1", "agent_2", {"task": "compute"})
        assert msg_id is not None

        msg = await bus.receive("agent_2", timeout=1.0)
        assert msg is not None
        assert msg.content == {"task": "compute"}
        assert msg.delivery_status == DeliveryStatus.DELIVERED

    async def test_broadcast(self):
        bus = MessageBus()
        bus.register_agent("sender")
        bus.register_agent("receiver_1")
        bus.register_agent("receiver_2")

        msg_id = await bus.broadcast("sender", {"announcement": "hello"})
        assert msg_id is not None

        msg1 = await bus.receive("receiver_1", timeout=1.0)
        msg2 = await bus.receive("receiver_2", timeout=1.0)

        assert msg1 is not None
        assert msg2 is not None
        assert msg1.content["announcement"] == "hello"
        assert msg2.content["announcement"] == "hello"

    async def test_priority_ordering(self):
        bus = MessageBus()
        bus.register_agent("sender")
        bus.register_agent("receiver")

        p3 = MessagePriority.P3_LOW.value
        p0 = MessagePriority.P0_CRITICAL.value
        await bus.send("sender", "receiver", {"p": "low"}, priority=p3)
        await bus.send("sender", "receiver", {"p": "high"}, priority=p0)

        first = await bus.receive("receiver", timeout=1.0)
        assert first.content == {"p": "high"}

    async def test_send_to_unregistered_raises(self):
        bus = MessageBus()
        bus.register_agent("a")
        with pytest.raises(ValueError, match="not registered"):
            await bus.send("a", "unknown", {"data": "test"})

    async def test_receive_from_unregistered_raises(self):
        bus = MessageBus()
        with pytest.raises(ValueError, match="not registered"):
            await bus.receive("unknown")

    async def test_receive_timeout_returns_none(self):
        bus = MessageBus()
        bus.register_agent("a")
        msg = await bus.receive("a", timeout=0.05)
        assert msg is None

    async def test_dead_letter_queue_on_unregister(self):
        bus = MessageBus()
        bus.register_agent("sender")
        bus.register_agent("receiver")

        await bus.send("sender", "receiver", {"task": "x"})
        bus.unregister_agent("receiver")

        dlq = bus.get_dead_letter_queue()
        assert len(dlq) == 1
        assert dlq[0].delivery_status == DeliveryStatus.DEAD_LETTER

    async def test_clear_dead_letter_queue(self):
        bus = MessageBus()
        bus.register_agent("a")
        bus.register_agent("b")
        await bus.send("a", "b", {"x": 1})
        bus.unregister_agent("b")

        count = bus.clear_dead_letter_queue()
        assert count == 1
        assert len(bus.get_dead_letter_queue()) == 0

    async def test_get_stats(self):
        bus = MessageBus(max_delivery_attempts=5, default_ttl=120.0)
        bus.register_agent("a")
        bus.register_agent("b")
        await bus.send("a", "b", {"data": "test"})

        stats = bus.get_stats()
        assert stats["registered_agents"] == 2
        assert stats["total_pending_messages"] == 1
        assert stats["dead_letter_count"] == 0
        assert stats["max_delivery_attempts"] == 5
        assert stats["default_ttl"] == 120.0

    async def test_pending_count(self):
        bus = MessageBus()
        bus.register_agent("a")
        bus.register_agent("b")

        await bus.send("a", "b", {"x": 1})
        await bus.send("a", "b", {"x": 2})

        assert bus.get_pending_count("b") == 2

    async def test_handle_failed_delivery(self):
        bus = MessageBus(max_delivery_attempts=2)
        bus.register_agent("a")
        bus.register_agent("b")

        msg = Message(from_agent="a", to_agent="b", content={"retry": True})
        await bus.handle_failed_delivery(msg)
        assert msg.delivery_attempts == 1
        assert msg.delivery_status == DeliveryStatus.PENDING

        await bus.handle_failed_delivery(msg)
        assert msg.delivery_status == DeliveryStatus.DEAD_LETTER

    async def test_concurrent_send_and_receive_no_message_loss(self):
        """Stress test: concurrent sends to same agent should not lose any messages.

        The race condition exists because send() enqueues then sets event separately.
        A waiting receive() could clear the event between sends, missing the signal
        for a message that was already enqueued. This test verifies the fix: every
        enqueued message reliably triggers the event under the same lock.
        """
        import asyncio
        bus = MessageBus()
        bus.register_agent("receiver")
        for i in range(100):
            bus.register_agent(f"sender_{i}")

        # Fire many sends concurrently, while a consumer drains
        send_tasks = [bus.send(f"sender_{i}", "receiver", {"i": i}) for i in range(100)]

        received = []
        stop_event = asyncio.Event()

        async def consumer():
            while not stop_event.is_set():
                msg = await bus.receive("receiver", timeout=0.5)
                if msg is not None:
                    received.append(msg.content)

        async def producer():
            await asyncio.gather(*send_tasks)

        consumer_task = asyncio.create_task(consumer())
        await producer()
        await asyncio.sleep(0.1)  # let consumer drain
        stop_event.set()
        await consumer_task

        assert len(received) == 100

    async def test_delivery_confirmation(self):
        bus = MessageBus()
        bus.register_agent("a")
        bus.register_agent("b")

        msg_id = await bus.send("a", "b", {"task": "x"}, require_confirmation=True)

        msg = await bus.receive("b", timeout=1.0)
        assert msg is not None

        # receive marks as delivered but NOT yet confirmed
        confirmed = await bus.check_delivery_confirmation(msg_id)
        assert confirmed is False

        # explicit confirm
        await bus.confirm_delivery("b", msg_id)
        confirmed = await bus.check_delivery_confirmation(msg_id)
        assert confirmed is True

    async def test_broadcast_unique_message_ids(self):
        """Each broadcast target should get a unique message_id copy."""
        bus = MessageBus()
        bus.register_agent("sender")
        bus.register_agent("r1")
        bus.register_agent("r2")

        msg_id = await bus.broadcast("sender", {"event": "hello"})

        msg1 = await bus.receive("r1", timeout=1.0)
        msg2 = await bus.receive("r2", timeout=1.0)

        assert msg1 is not None
        assert msg2 is not None
        # Each target gets its own unique message_id
        assert msg1.message_id != msg2.message_id
        # The returned msg_id from broadcast is the first target's
        assert msg_id == msg1.message_id

    async def test_delivery_confirmation_not_tracked_by_default(self):
        """Messages sent without require_confirmation should not have confirmation tracking."""
        bus = MessageBus()
        bus.register_agent("a")
        bus.register_agent("b")

        msg_id = await bus.send("a", "b", {"task": "x"})
        await bus.receive("b", timeout=1.0)

        # Not tracked, returns None
        confirmed = await bus.check_delivery_confirmation(msg_id)
        assert confirmed is None

    async def test_request_response_basic(self):
        """Request-response pattern: send request, await reply on dedicated channel."""
        bus = MessageBus()
        bus.register_agent("client")
        bus.register_agent("server")

        # Client sends request and awaits response
        req_id = await bus.request("client", "server", {"query": "sum", "values": [1, 2, 3]}, timeout=1.0)

        # Server receives request
        msg = await bus.receive("server", timeout=1.0)
        assert msg is not None
        assert msg.content == {"query": "sum", "values": [1, 2, 3]}

        # Server sends reply back
        await bus.reply("server", "client", req_id, {"result": 6})

        # Client receives response
        response = await bus.receive_response("client", req_id, timeout=1.0)
        assert response is not None
        assert response.content == {"result": 6}

    async def test_request_response_timeout(self):
        """Request-response times out when no reply arrives."""
        bus = MessageBus()
        bus.register_agent("client")
        bus.register_agent("server")

        req_id = await bus.request("client", "server", {"data": "test"}, timeout=0.5)

        # No reply sent — should timeout
        response = await bus.receive_response("client", req_id, timeout=0.1)
        assert response is None

    async def test_request_response_multiple_requests(self):
        """Multiple concurrent requests should not mix up responses."""
        bus = MessageBus()
        bus.register_agent("c1")
        bus.register_agent("c2")
        bus.register_agent("server")

        req1 = await bus.request("c1", "server", {"from": "c1"}, timeout=1.0)
        req2 = await bus.request("c2", "server", {"from": "c2"}, timeout=1.0)

        # Server processes in order
        await bus.receive("server", timeout=1.0)
        await bus.receive("server", timeout=1.0)

        await bus.reply("server", "c2", req2, {"reply_to": "c2"})
        await bus.reply("server", "c1", req1, {"reply_to": "c1"})

        resp1 = await bus.receive_response("c1", req1, timeout=1.0)
        resp2 = await bus.receive_response("c2", req2, timeout=1.0)

        assert resp1.content == {"reply_to": "c1"}
        assert resp2.content == {"reply_to": "c2"}


# --- Agent Metadata Tests ---


@pytest.mark.asyncio
class TestAgentMetadata:
    async def test_register_agent_with_metadata(self):
        """Register an agent with metadata and retrieve it."""
        bus = MessageBus()
        bus.register_agent("explorer", metadata={"role": "search", "version": "1.0"})
        meta = bus.get_agent_metadata("explorer")
        assert meta == {"role": "search", "version": "1.0"}

    async def test_register_agent_without_metadata(self):
        """Backwards compatible: register without metadata defaults to empty dict."""
        bus = MessageBus()
        bus.register_agent("agent_1")
        meta = bus.get_agent_metadata("agent_1")
        assert meta == {}

    async def test_get_agent_metadata_unknown_agent(self):
        """get_agent_metadata for an unknown agent returns empty dict."""
        bus = MessageBus()
        bus.register_agent("a")
        meta = bus.get_agent_metadata("unknown")
        assert meta == {}

    async def test_list_agents(self):
        """list_agents returns list of IDs by default."""
        bus = MessageBus()
        bus.register_agent("alpha")
        bus.register_agent("beta")
        agents = bus.list_agents()
        assert sorted(agents) == ["alpha", "beta"]

    async def test_list_agents_with_metadata(self):
        """list_agents(include_metadata=True) returns dict with metadata."""
        bus = MessageBus()
        bus.register_agent("alpha", metadata={"role": "search"})
        bus.register_agent("beta")
        result = bus.list_agents(include_metadata=True)
        assert result == {
            "alpha": {"role": "search"},
            "beta": {},
        }

    async def test_unregister_agent_clears_metadata(self):
        """Unregistering an agent removes its metadata."""
        bus = MessageBus()
        bus.register_agent("temp", metadata={"role": "worker"})
        bus.unregister_agent("temp")
        meta = bus.get_agent_metadata("temp")
        assert meta == {}

    async def test_reregister_agent_replaces_metadata(self):
        """Re-registering an agent replaces its metadata."""
        bus = MessageBus()
        bus.register_agent("worker", metadata={"role": "search"})
        bus.register_agent("worker", metadata={"role": "execute"})
        meta = bus.get_agent_metadata("worker")
        assert meta == {"role": "execute"}

    async def test_stats_include_agent_count(self):
        """get_stats includes registered agent count."""
        bus = MessageBus(max_delivery_attempts=5, default_ttl=120.0)
        bus.register_agent("a", metadata={"role": "client"})
        bus.register_agent("b")
        await bus.send("a", "b", {"data": "test"})

        stats = bus.get_stats()
        assert stats["registered_agents"] == 2
        assert stats["total_pending_messages"] == 1
        assert stats["dead_letter_count"] == 0
        assert stats["max_delivery_attempts"] == 5
        assert stats["default_ttl"] == 120.0
