"""Priority-based message routing with per-agent queues."""

import asyncio
import heapq
from collections import defaultdict
from typing import Dict, List, Optional

from agent_message_bus.message import Message


class MessageRouter:
    """
    Message routing infrastructure.

    Features:
    - Priority-based message ordering per agent queue
    - Agent availability checking
    - Broadcast and unicast routing decisions
    - Automatic expired message skipping
    """

    def __init__(self):
        """Initialize the message router."""
        self._agent_queues: Dict[str, List[Message]] = defaultdict(list)
        self._agent_available: Dict[str, bool] = {}
        self._lock = asyncio.Lock()

    async def is_agent_available(self, agent_id: str) -> bool:
        """Check if agent is registered and available for delivery."""
        async with self._lock:
            return self._agent_available.get(agent_id, False)

    async def set_agent_availability(self, agent_id: str, available: bool) -> None:
        """Set agent availability status."""
        async with self._lock:
            self._agent_available[agent_id] = available

    def set_agent_availability_sync(self, agent_id: str, available: bool) -> None:
        """Synchronous version for sync usage patterns."""
        self._agent_available[agent_id] = available

    async def route_message(self, message: Message, registered_agents: List[str]) -> List[str]:
        """
        Determine target agents for a message.

        Args:
            message: Message to route
            registered_agents: List of currently registered agent IDs

        Returns:
            List of target agent IDs for delivery
        """
        async with self._lock:
            if message.is_broadcast():
                return [a for a in registered_agents if a != message.from_agent]
            else:
                return [message.to_agent] if message.to_agent in registered_agents else []

    async def enqueue_for_agent(self, agent_id: str, message: Message) -> bool:
        """
        Add message to agent's priority queue.

        Args:
            agent_id: Target agent ID
            message: Message to enqueue

        Returns:
            True if enqueued successfully
        """
        async with self._lock:
            if message.is_expired():
                return False
            heapq.heappush(self._agent_queues[agent_id], message)
            return True

    def enqueue_for_agent_sync(self, agent_id: str, message: Message) -> bool:
        """Synchronous version of enqueue."""
        if message.is_expired():
            return False
        heapq.heappush(self._agent_queues[agent_id], message)
        return True

    async def get_next_for_agent(self, agent_id: str) -> Optional[Message]:
        """
        Get next highest priority message for agent, skipping expired ones.

        Args:
            agent_id: Agent ID

        Returns:
            Next message or None if queue empty
        """
        async with self._lock:
            return self._pop_next(agent_id)

    def get_next_for_agent_sync(self, agent_id: str) -> Optional[Message]:
        """Synchronous version of get_next."""
        return self._pop_next(agent_id)

    def _pop_next(self, agent_id: str) -> Optional[Message]:
        """Internal: pop next non-expired message from queue."""
        queue = self._agent_queues.get(agent_id, [])
        while queue:
            message = heapq.heappop(queue)
            if not message.is_expired():
                return message
        return None

    async def get_queue_depth(self, agent_id: str) -> int:
        """Get number of pending messages for agent."""
        async with self._lock:
            return len(self._agent_queues.get(agent_id, []))

    def get_queue_depth_sync(self, agent_id: str) -> int:
        """Synchronous version of get_queue_depth."""
        return len(self._agent_queues.get(agent_id, []))

    async def clear_agent_queue(self, agent_id: str) -> List[Message]:
        """Clear all pending messages for agent."""
        async with self._lock:
            messages = self._agent_queues.get(agent_id, [])
            self._agent_queues[agent_id] = []
            return list(messages)

    def clear_agent_queue_sync(self, agent_id: str) -> List[Message]:
        """Synchronous version of clear_agent_queue."""
        messages = self._agent_queues.get(agent_id, [])
        self._agent_queues[agent_id] = []
        return messages
