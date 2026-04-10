"""
agent-message-bus: Lightweight agent-to-agent messaging infrastructure.

Features:
- Priority-based message routing (P0-P3)
- Message TTL with automatic expiration
- Dead letter queue for failed deliveries
- Delivery confirmation
- Broadcast and point-to-point messaging
- Pluggable storage backend (InMemory, Redis, etc.)
- Async-first with sync compatibility

Zero external dependencies (Redis backend is optional).

Example:
    >>> from agent_message_bus import MessageBus, MessagePriority
    >>> bus = MessageBus()
    >>> bus.register_agent("agent_1")
    >>> bus.register_agent("agent_2")
    >>> await bus.send("agent_1", "agent_2", {"task": "compute"}, priority=MessagePriority.P1_HIGH)
    >>> msg = await bus.receive("agent_2", timeout=5.0)
    >>> msg.content
    {'task': 'compute'}
"""

from agent_message_bus.backend import Backend, InMemoryBackend
from agent_message_bus.bus import MessageBus
from agent_message_bus.message import (
    DeliveryStatus,
    Message,
    MessagePriority,
)
from agent_message_bus.router import MessageRouter

__version__ = "0.2.0"

__all__ = [
    "Backend",
    "InMemoryBackend",
    "Message",
    "MessagePriority",
    "DeliveryStatus",
    "MessageRouter",
    "MessageBus",
]
