# agent-message-bus

[![PyPI version](https://badge.fury.io/py/agent-message-bus.svg)](https://badge.fury.io/py/agent-message-bus)
[![Python](https://img.shields.io/pypi/pyversions/agent-message-bus.svg)](https://pypi.org/project/agent-message-bus/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Lightweight **agent-to-agent messaging** infrastructure with priority routing, TTL, dead letter queues, and delivery confirmation. Zero external dependencies.

## Features

| Feature | Description |
|---------|-------------|
| **Priority Routing** | 4-level priority (P0_CRITICAL → P3_LOW) with automatic ordering |
| **Point-to-Point** | Direct messaging between registered agents |
| **Broadcast** | One-to-many messaging to all registered agents |
| **Message TTL** | Auto-expire stale messages |
| **Dead Letter Queue** | Track and recover failed deliveries |
| **Delivery Confirmation** | Optional end-to-end delivery acknowledgment |
| **Async-First** | Built on `asyncio` with sync compatibility |
| **Zero Dependencies** | Only Python standard library |

## Installation

```bash
pip install agent-message-bus
```

## Quick Start

```python
import asyncio
from agent_message_bus import MessageBus, MessagePriority

async def main():
    bus = MessageBus()

    # Register agents
    bus.register_agent("explorer")
    bus.register_agent("executor")
    bus.register_agent("validator")

    # Send a high-priority task
    await bus.send(
        from_agent="explorer",
        to_agent="executor",
        message={"task": "analyze_data", "dataset": "users.csv"},
        priority=MessagePriority.P1_HIGH,
    )

    # Receive on the executor side
    msg = await bus.receive("executor", timeout=5.0)
    print(f"Received: {msg.content}")
    # → Received: {'task': 'analyze_data', 'dataset': 'users.csv'}

    # Broadcast to all agents
    await bus.broadcast("explorer", {"event": "discovery_complete"})

asyncio.run(main())
```

## Priority Levels

```python
from agent_message_bus import MessagePriority

MessagePriority.P0_CRITICAL  # Emergency, safety-related
MessagePriority.P1_HIGH      # Task coordination
MessagePriority.P2_NORMAL    # Regular communication
MessagePriority.P3_LOW       # Status updates, logs
```

Messages with lower priority values are always dequeued first. Within the same priority, earlier messages are delivered first.

## Advanced Usage

### Message TTL

```python
await bus.send("a", "b", {"alert": "disk full"}, ttl=30.0)  # expires in 30s
```

### Delivery Confirmation

```python
msg_id = await bus.send("a", "b", {"task": "compute"}, require_confirmation=True)

# After receive...
confirmed = await bus.check_delivery_confirmation(msg_id)
print(f"Delivered: {confirmed}")  # True
```

### Dead Letter Queue

```python
dlq = bus.get_dead_letter_queue()
for msg in dlq:
    print(f"Failed: {msg.message_id} - attempts: {msg.delivery_attempts}")

bus.clear_dead_letter_queue()
```

### Statistics

```python
stats = bus.get_stats()
print(stats)
# {
#   "registered_agents": 3,
#   "total_pending_messages": 5,
#   "dead_letter_count": 1,
#   "max_delivery_attempts": 3,
#   "default_ttl": 60.0
# }
```

## Architecture

```
Agent A ──┐
           ├─► MessageRouter ──► Agent B Queue (priority heap)
Agent C ──┘

MessageBus manages:
  ├── Agent registration / lifecycle
  ├── Point-to-point + broadcast routing
  ├── Dead Letter Queue (failed/expired)
  └── Delivery confirmation tracking
```

## API Reference

### `Message`

| Attribute | Type | Description |
|-----------|------|-------------|
| `message_id` | `str` | Auto-generated UUID |
| `from_agent` | `str` | Source agent ID |
| `to_agent` | `str` | Target agent ID or `"broadcast"` |
| `content` | `dict` | Message payload |
| `priority` | `int` | Priority level (see `MessagePriority`) |
| `ttl` | `float` | Time-to-live in seconds (0 = no expiry) |
| `delivery_status` | `DeliveryStatus` | Current delivery state |

### `MessageBus`

| Method | Description |
|--------|-------------|
| `register_agent(id)` | Register an agent |
| `unregister_agent(id)` | Unregister (pending messages → DLQ) |
| `send(from, to, content, ...)` | Point-to-point message |
| `broadcast(from, content, ...)` | Broadcast to all agents |
| `receive(agent, timeout)` | Receive next message |
| `get_dead_letter_queue()` | Get failed messages |
| `get_stats()` | Bus statistics |

## License

MIT
