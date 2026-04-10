"""Tests for pluggable backend abstraction."""

import asyncio
import time

import pytest

from agent_message_bus.backend import Backend, InMemoryBackend
from agent_message_bus.message import Message, MessagePriority


# --- Backend Contract Tests (run against InMemoryBackend) ---


class TestBackendContract:
    """Contract tests that any Backend implementation must pass."""

    @pytest.fixture
    async def backend(self) -> InMemoryBackend:
        b = InMemoryBackend()
        await b.initialize()
        yield b
        await b.close()

    async def test_enqueue_returns_true(self, backend: Backend):
        msg = Message(from_agent="a", to_agent="b", content={"x": 1})
        result = await backend.enqueue("b", msg)
        assert result is True

    async def test_enqueue_rejects_expired(self, backend: Backend):
        msg = Message(from_agent="a", to_agent="b", content={}, ttl=0.001)
        time.sleep(0.01)
        result = await backend.enqueue("b", msg)
        assert result is False

    async def test_dequeue_returns_message(self, backend: Backend):
        msg = Message(from_agent="a", to_agent="b", content={"task": "x"})
        await backend.enqueue("b", msg)
        result = await backend.dequeue("b")
        assert result is not None
        assert result.content == {"task": "x"}

    async def test_dequeue_empty_returns_none(self, backend: Backend):
        result = await backend.dequeue("nonexistent")
        assert result is None

    async def test_priority_ordering(self, backend: Backend):
        p3 = MessagePriority.P3_LOW.value
        p0 = MessagePriority.P0_CRITICAL.value
        low = Message(from_agent="a", to_agent="b", content={"p": "low"}, priority=p3)
        high = Message(from_agent="a", to_agent="b", content={"p": "high"}, priority=p0)

        await backend.enqueue("b", low)
        await backend.enqueue("b", high)

        first = await backend.dequeue("b")
        assert first is not None
        assert first.content == {"p": "high"}

        second = await backend.dequeue("b")
        assert second is not None
        assert second.content == {"p": "low"}

    async def test_fifo_within_same_priority(self, backend: Backend):
        p2 = MessagePriority.P2_NORMAL.value
        msg1 = Message(from_agent="a", to_agent="b", content={"seq": 1}, priority=p2)
        await backend.enqueue("b", msg1)
        # Small delay to ensure different timestamps
        await asyncio.sleep(0.01)
        msg2 = Message(from_agent="a", to_agent="b", content={"seq": 2}, priority=p2)
        await backend.enqueue("b", msg2)

        first = await backend.dequeue("b")
        assert first is not None
        assert first.content == {"seq": 1}

    async def test_dequeue_skips_expired_in_queue(self, backend: Backend):
        # Enqueue an expired message (bypasses enqueue guard via sync path)
        expired_msg = Message(
            from_agent="a", to_agent="b", content={"expired": True},
            priority=MessagePriority.P0_CRITICAL.value,
        )
        expired_msg.expires_at = expired_msg.timestamp  # already expired
        backend.enqueue_sync("b", expired_msg)

        # Enqueue a valid message
        valid_msg = Message(from_agent="a", to_agent="b", content={"valid": True})
        await backend.enqueue("b", valid_msg)

        # dequeue should skip expired and return valid
        result = await backend.dequeue("b")
        assert result is not None
        assert result.content == {"valid": True}

    async def test_get_queue_depth(self, backend: Backend):
        await backend.enqueue("b", Message(from_agent="a", to_agent="b", content={}))
        await backend.enqueue("b", Message(from_agent="a", to_agent="b", content={}))
        depth = await backend.get_queue_depth("b")
        assert depth == 2

    async def test_get_queue_depth_nonexistent(self, backend: Backend):
        depth = await backend.get_queue_depth("nonexistent")
        assert depth == 0

    async def test_clear_queue(self, backend: Backend):
        await backend.enqueue("b", Message(from_agent="a", to_agent="b", content={"x": 1}))
        await backend.enqueue("b", Message(from_agent="a", to_agent="b", content={"x": 2}))
        cleared = await backend.clear_queue("b")
        assert len(cleared) == 2
        depth = await backend.get_queue_depth("b")
        assert depth == 0

    async def test_clear_queue_nonexistent(self, backend: Backend):
        cleared = await backend.clear_queue("nonexistent")
        assert cleared == []

    async def test_isolated_queues_per_agent(self, backend: Backend):
        await backend.enqueue("b1", Message(from_agent="a", to_agent="b1", content={"agent": "b1"}))
        await backend.enqueue("b2", Message(from_agent="a", to_agent="b2", content={"agent": "b2"}))

        msg_b1 = await backend.dequeue("b1")
        msg_b2 = await backend.dequeue("b2")

        assert msg_b1.content == {"agent": "b1"}
        assert msg_b2.content == {"agent": "b2"}


class TestInMemoryBackendSync:
    """Test InMemoryBackend synchronous methods."""

    @pytest.fixture
    def backend(self) -> InMemoryBackend:
        return InMemoryBackend()

    def test_enqueue_sync(self, backend: InMemoryBackend):
        msg = Message(from_agent="a", to_agent="b", content={"x": 1})
        result = backend.enqueue_sync("b", msg)
        assert result is True

    def test_enqueue_sync_rejects_expired(self, backend: InMemoryBackend):
        msg = Message(from_agent="a", to_agent="b", content={}, ttl=0.001)
        time.sleep(0.01)
        result = backend.enqueue_sync("b", msg)
        assert result is False

    def test_dequeue_sync(self, backend: InMemoryBackend):
        msg = Message(from_agent="a", to_agent="b", content={"task": "x"})
        backend.enqueue_sync("b", msg)
        result = backend.dequeue_sync("b")
        assert result is not None
        assert result.content == {"task": "x"}

    def test_dequeue_sync_empty(self, backend: InMemoryBackend):
        result = backend.dequeue_sync("nonexistent")
        assert result is None

    def test_queue_depth_sync(self, backend: InMemoryBackend):
        backend.enqueue_sync("b", Message(from_agent="a", to_agent="b", content={}))
        backend.enqueue_sync("b", Message(from_agent="a", to_agent="b", content={}))
        assert backend.get_queue_depth_sync("b") == 2

    def test_clear_queue_sync(self, backend: InMemoryBackend):
        backend.enqueue_sync("b", Message(from_agent="a", to_agent="b", content={"x": 1}))
        cleared = backend.clear_queue_sync("b")
        assert len(cleared) == 1
        assert backend.get_queue_depth_sync("b") == 0


# --- MessageBus with custom backend ---


@pytest.mark.asyncio
class TestMessageBusWithBackend:
    """Test MessageBus accepting a custom backend instance."""

    async def test_message_bus_with_inmemory_backend(self):
        from agent_message_bus.bus import MessageBus
        from agent_message_bus.backend import InMemoryBackend

        backend = InMemoryBackend()
        await backend.initialize()
        bus = MessageBus(backend=backend)
        bus.register_agent("agent_1")
        bus.register_agent("agent_2")

        msg_id = await bus.send("agent_1", "agent_2", {"task": "compute"})
        assert msg_id is not None

        msg = await bus.receive("agent_2", timeout=1.0)
        assert msg is not None
        assert msg.content == {"task": "compute"}

        await backend.close()

    async def test_message_bus_default_backend(self):
        """MessageBus without explicit backend should work with default InMemoryBackend."""
        from agent_message_bus.bus import MessageBus

        bus = MessageBus()
        bus.register_agent("a")
        bus.register_agent("b")

        msg_id = await bus.send("a", "b", {"data": "test"})
        assert msg_id is not None

        msg = await bus.receive("b", timeout=1.0)
        assert msg is not None
        assert msg.content == {"data": "test"}

    async def test_message_bus_lifecycle_initialize_close(self):
        """MessageBus initialize/close should delegate to backend."""
        from agent_message_bus.bus import MessageBus
        from agent_message_bus.backend import InMemoryBackend

        backend = InMemoryBackend()
        bus = MessageBus(backend=backend)
        bus.register_agent("a")

        await bus.initialize()
        assert backend._initialized is True

        await bus.close()
        assert backend._initialized is False

    async def test_message_bus_priority_ordering_with_custom_backend(self):
        from agent_message_bus.bus import MessageBus
        from agent_message_bus.backend import InMemoryBackend

        backend = InMemoryBackend()
        await backend.initialize()
        bus = MessageBus(backend=backend)
        bus.register_agent("sender")
        bus.register_agent("receiver")

        p3 = MessagePriority.P3_LOW.value
        p0 = MessagePriority.P0_CRITICAL.value
        await bus.send("sender", "receiver", {"p": "low"}, priority=p3)
        await bus.send("sender", "receiver", {"p": "high"}, priority=p0)

        first = await bus.receive("receiver", timeout=1.0)
        assert first.content == {"p": "high"}

        await backend.close()

    async def test_message_bus_dead_letter_with_custom_backend(self):
        from agent_message_bus.bus import MessageBus
        from agent_message_bus.backend import InMemoryBackend
        from agent_message_bus.message import DeliveryStatus

        backend = InMemoryBackend()
        await backend.initialize()
        bus = MessageBus(backend=backend)
        bus.register_agent("sender")
        bus.register_agent("receiver")

        await bus.send("sender", "receiver", {"task": "x"})
        bus.unregister_agent("receiver")

        dlq = bus.get_dead_letter_queue()
        assert len(dlq) == 1
        assert dlq[0].delivery_status == DeliveryStatus.DEAD_LETTER

        await backend.close()

    async def test_message_bus_stats_with_custom_backend(self):
        from agent_message_bus.bus import MessageBus
        from agent_message_bus.backend import InMemoryBackend

        backend = InMemoryBackend()
        await backend.initialize()
        bus = MessageBus(backend=backend)
        bus.register_agent("a")
        bus.register_agent("b")
        await bus.send("a", "b", {"data": "test"})

        stats = bus.get_stats()
        assert stats["registered_agents"] == 2
        assert stats["total_pending_messages"] == 1

        await backend.close()


# --- RedisBackend Tests (skipped if redis not available) ---


@pytest.mark.asyncio
class TestRedisBackend:
    """Tests for RedisBackend using a real or mocked Redis."""

    @pytest.fixture
    async def backend(self):
        redis = pytest.importorskip("redis", reason="redis package not installed")
        from agent_message_bus.backend import RedisBackend

        b = RedisBackend(redis_url="redis://localhost:6379/15", key_prefix="test_bus")
        try:
            await b.initialize()
            yield b
        except Exception:
            pytest.skip("Redis not available at localhost:6379")
        finally:
            await b.close()

    async def test_enqueue_returns_true(self, backend):
        msg = Message(from_agent="a", to_agent="b", content={"x": 1})
        result = await backend.enqueue("b", msg)
        assert result is True

    async def test_enqueue_rejects_expired(self, backend):
        msg = Message(from_agent="a", to_agent="b", content={}, ttl=0.001)
        time.sleep(0.01)
        result = await backend.enqueue("b", msg)
        assert result is False

    async def test_dequeue_returns_message(self, backend):
        msg = Message(from_agent="a", to_agent="b", content={"task": "x"})
        await backend.enqueue("b", msg)
        result = await backend.dequeue("b")
        assert result is not None
        assert result.content == {"task": "x"}

    async def test_dequeue_empty_returns_none(self, backend):
        result = await backend.dequeue("nonexistent")
        assert result is None

    async def test_priority_ordering(self, backend):
        p3 = MessagePriority.P3_LOW.value
        p0 = MessagePriority.P0_CRITICAL.value
        low = Message(from_agent="a", to_agent="b", content={"p": "low"}, priority=p3)
        high = Message(from_agent="a", to_agent="b", content={"p": "high"}, priority=p0)

        await backend.enqueue("b", low)
        await backend.enqueue("b", high)

        first = await backend.dequeue("b")
        assert first is not None
        assert first.content == {"p": "high"}

    async def test_fifo_within_same_priority(self, backend):
        p2 = MessagePriority.P2_NORMAL.value
        msg1 = Message(from_agent="a", to_agent="b", content={"seq": 1}, priority=p2)
        await backend.enqueue("b", msg1)
        await asyncio.sleep(0.01)
        msg2 = Message(from_agent="a", to_agent="b", content={"seq": 2}, priority=p2)
        await backend.enqueue("b", msg2)

        first = await backend.dequeue("b")
        assert first is not None
        assert first.content == {"seq": 1}

    async def test_get_queue_depth(self, backend):
        await backend.enqueue("b", Message(from_agent="a", to_agent="b", content={}))
        await backend.enqueue("b", Message(from_agent="a", to_agent="b", content={}))
        depth = await backend.get_queue_depth("b")
        assert depth == 2

    async def test_clear_queue(self, backend):
        await backend.enqueue("b", Message(from_agent="a", to_agent="b", content={"x": 1}))
        await backend.enqueue("b", Message(from_agent="a", to_agent="b", content={"x": 2}))
        cleared = await backend.clear_queue("b")
        assert len(cleared) == 2
        depth = await backend.get_queue_depth("b")
        assert depth == 0

    async def test_isolated_queues_per_agent(self, backend):
        await backend.enqueue("b1", Message(from_agent="a", to_agent="b1", content={"agent": "b1"}))
        await backend.enqueue("b2", Message(from_agent="a", to_agent="b2", content={"agent": "b2"}))

        msg_b1 = await backend.dequeue("b1")
        msg_b2 = await backend.dequeue("b2")

        assert msg_b1.content == {"agent": "b1"}
        assert msg_b2.content == {"agent": "b2"}
