"""Pluggable backend abstraction for agent message queues."""

import asyncio
import heapq
import json
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, List, Optional

from agent_message_bus.message import Message


class Backend(ABC):
    """Abstract base class for message queue backends."""

    @abstractmethod
    async def enqueue(self, agent_id: str, message: Message) -> bool:
        """Add message to agent's priority queue. Returns False if message is expired."""

    @abstractmethod
    async def dequeue(self, agent_id: str) -> Optional[Message]:
        """Get next highest priority message, skipping expired ones. Returns None if empty."""

    @abstractmethod
    async def get_queue_depth(self, agent_id: str) -> int:
        """Get number of pending messages for agent."""

    @abstractmethod
    async def clear_queue(self, agent_id: str) -> List[Message]:
        """Clear all pending messages for agent. Returns cleared messages."""

    async def initialize(self) -> None:
        """Setup backend resources. Override for connection setup."""

    async def close(self) -> None:
        """Cleanup backend resources. Override for connection teardown."""


class InMemoryBackend(Backend):
    """In-memory backend using heapq + asyncio.Lock. Zero external dependencies."""

    def __init__(self) -> None:
        self._queues: Dict[str, List[Message]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self) -> None:
        self._initialized = True

    async def close(self) -> None:
        self._initialized = False

    async def enqueue(self, agent_id: str, message: Message) -> bool:
        if message.is_expired():
            return False
        async with self._lock:
            heapq.heappush(self._queues[agent_id], message)
        return True

    def enqueue_sync(self, agent_id: str, message: Message) -> bool:
        """Synchronous enqueue for sync usage patterns."""
        if message.is_expired():
            return False
        heapq.heappush(self._queues[agent_id], message)
        return True

    async def dequeue(self, agent_id: str) -> Optional[Message]:
        async with self._lock:
            return self._pop_next(agent_id)

    def dequeue_sync(self, agent_id: str) -> Optional[Message]:
        """Synchronous dequeue for sync usage patterns."""
        return self._pop_next(agent_id)

    def _pop_next(self, agent_id: str) -> Optional[Message]:
        """Internal: pop next non-expired message from queue."""
        queue = self._queues.get(agent_id, [])
        while queue:
            message = heapq.heappop(queue)
            if not message.is_expired():
                return message
        return None

    async def get_queue_depth(self, agent_id: str) -> int:
        async with self._lock:
            return len(self._queues.get(agent_id, []))

    def get_queue_depth_sync(self, agent_id: str) -> int:
        """Synchronous queue depth."""
        return len(self._queues.get(agent_id, []))

    async def clear_queue(self, agent_id: str) -> List[Message]:
        async with self._lock:
            messages = self._queues.get(agent_id, [])
            self._queues[agent_id] = []
            return list(messages)

    def clear_queue_sync(self, agent_id: str) -> List[Message]:
        """Synchronous clear queue."""
        messages = self._queues.get(agent_id, [])
        self._queues[agent_id] = []
        return messages


class RedisBackend(Backend):
    """Redis backend using sorted sets for priority ordering.

    Score = priority * 1e12 + timestamp_ns to ensure:
    - Lower priority value dequeued first
    - FIFO within same priority (earlier timestamp first)

    Requires: pip install agent-message-bus[redis]
    """

    _SCORE_MULTIPLIER = 1e12

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        key_prefix: str = "agent_bus",
    ) -> None:
        self._redis_url = redis_url
        self._key_prefix = key_prefix
        self._client = None
        self._initialized = False

    def _queue_key(self, agent_id: str) -> str:
        return f"{self._key_prefix}:queue:{agent_id}"

    def _member_key(self, message: Message) -> str:
        return f"{self._key_prefix}:msg:{message.message_id}"

    def _score(self, message: Message) -> float:
        ts = message.timestamp.timestamp()
        return message.priority * self._SCORE_MULTIPLIER + ts

    async def initialize(self) -> None:
        import redis.asyncio

        self._client = redis.asyncio.from_url(self._redis_url)
        await self._client.ping()
        self._initialized = True

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
        self._initialized = False

    async def enqueue(self, agent_id: str, message: Message) -> bool:
        if message.is_expired():
            return False
        assert self._client is not None
        key = self._queue_key(agent_id)
        member = self._member_key(message)
        score = self._score(message)
        serialized = json.dumps(message.to_dict(), default=str)
        await self._client.zadd(key, {member: score})
        await self._client.hset(f"{self._key_prefix}:messages", member, serialized)
        return True

    async def dequeue(self, agent_id: str) -> Optional[Message]:
        assert self._client is not None
        key = self._queue_key(agent_id)
        messages_hash = f"{self._key_prefix}:messages"

        # Get lowest score member (highest priority)
        members = await self._client.zpopmin(key, 1)
        if not members:
            return None

        member_key, _score = members[0]
        member_key_str = member_key.decode() if isinstance(member_key, bytes) else member_key

        serialized = await self._client.hget(messages_hash, member_key_str)
        if serialized is None:
            return None

        data = json.loads(serialized)
        message = Message.from_dict(data)

        if message.is_expired():
            return await self.dequeue(agent_id)  # Skip expired, try next

        await self._client.hdel(messages_hash, member_key_str)
        return message

    async def get_queue_depth(self, agent_id: str) -> int:
        assert self._client is not None
        key = self._queue_key(agent_id)
        return await self._client.zcard(key)

    async def clear_queue(self, agent_id: str) -> List[Message]:
        assert self._client is not None
        key = self._queue_key(agent_id)
        messages_hash = f"{self._key_prefix}:messages"

        members = await self._client.zrange(key, 0, -1)
        if not members:
            return []

        messages = []
        for member_key in members:
            member_key_str = member_key.decode() if isinstance(member_key, bytes) else member_key
            serialized = await self._client.hget(messages_hash, member_key_str)
            if serialized:
                data = json.loads(serialized)
                messages.append(Message.from_dict(data))
            await self._client.hdel(messages_hash, member_key_str)

        await self._client.delete(key)
        return messages
