"""
Example: Multi-agent task coordination using agent-message-bus.

Demonstrates priority routing, broadcast, and delivery confirmation
in a simulated exploration pipeline.
"""

import asyncio
from agent_message_bus import MessageBus, MessagePriority


async def main():
    bus = MessageBus(default_ttl=30.0)

    # Register a 3-agent exploration pipeline
    bus.register_agent("explorer")
    bus.register_agent("analyst")
    bus.register_agent("reporter")

    print("=== Agent Message Bus Demo ===\n")

    # Step 1: Explorer sends findings to analyst
    print("1. Explorer → Analyst (high priority)")
    await bus.send(
        "explorer",
        "analyst",
        {
            "event": "discovery",
            "pattern": "unusual_traffic",
            "confidence": 0.85,
            "source": "network_segment_3",
        },
        priority=MessagePriority.P1_HIGH,
    )

    # Step 2: Broadcast status to all agents
    print("2. Explorer → Broadcast (normal priority)")
    await bus.broadcast("explorer", {"status": "exploration_complete"})

    # Step 3: Analyst processes and forwards to reporter
    analyst_msg = await bus.receive("analyst", timeout=1.0)
    print(f"   Analyst received: {analyst_msg.content}")

    print("3. Analyst → Reporter (normal priority)")
    await bus.send(
        "analyst",
        "reporter",
        {
            "event": "analysis_complete",
            "severity": "medium",
            "recommendation": "investigate",
        },
    )

    # Step 4: Reporter receives analysis and broadcast
    reporter_msg = await bus.receive("reporter", timeout=1.0)
    print(f"   Reporter received: {reporter_msg.content}")

    # Reporter also gets the broadcast
    reporter_broadcast = await bus.receive("reporter", timeout=0.5)
    if reporter_broadcast:
        print(f"   Reporter also got broadcast: {reporter_broadcast.content}")

    # Step 5: Explorer receives the analyst's broadcast reply
    # (broadcast excludes sender, so explorer won't see its own broadcast)
    analyst_broadcast = await bus.receive("analyst", timeout=0.5)
    if analyst_broadcast:
        print(f"   Analyst got broadcast too: {analyst_broadcast.content}")

    # Step 6: Check stats
    print("\n=== Bus Statistics ===")
    stats = bus.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")

    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
