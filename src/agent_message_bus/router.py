"""Priority-based message routing with pluggable backend."""

import asyncio
from typing import Dict, List, Optional

from agent_message_bus.backend import Backend, InMemoryBackend
from agent_message_bus.message import Message


class MessageRouter:
    """
    Message routing infrastructure.

    Delegates queue operations to a pluggable Backend instance.

    Features:
    - Priority-based message ordering per agent queue
    - Agent availability checking
    - Broadcast and unicast routing decisions
    - Automatic expired message skipping
    """

    def __init__(self, backend: Optional[Backend] = None):
        """
        Initialize the message router.

        Args:
            backend: Queue backend instance. Defaults to InMemoryBackend.
        """
        self._backend = backend or InMemoryBackend()
        self._agent_available: Dict[str, bool] = {}
        self._lock = asyncio.Lock()

    @property
    def backend(self) -> Backend:
        """Access the underlying backend instance."""
        return self._backend

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

    async def enqueue_for_agent(self, agent_id: str, message: Message, event: Optional[asyncio.Event] = None) -> bool:
        """
        Add message to agent's priority queue.

        Args:
            agent_id: Target agent ID
            message: Message to enqueue
            event: Optional asyncio.Event to set after enqueueing

        Returns:
            True if enqueued successfully
        """
        result = await self._backend.enqueue(agent_id, message)
        if result and event is not None:
            event.set()
        return result

    def enqueue_for_agent_sync(self, agent_id: str, message: Message) -> bool:
        """Synchronous version of enqueue."""
        if isinstance(self._backend, InMemoryBackend):
            return self._backend.enqueue_sync(agent_id, message)
        # For non-InMemory backends, fall back to a best-effort check
        if message.is_expired():
            return False
        return True

    async def get_next_for_agent(self, agent_id: str) -> Optional[Message]:
        """
        Get next highest priority message for agent, skipping expired ones.

        Args:
            agent_id: Agent ID

        Returns:
            Next message or None if queue empty
        """
        return await self._backend.dequeue(agent_id)

    def get_next_for_agent_sync(self, agent_id: str) -> Optional[Message]:
        """Synchronous version of get_next."""
        if isinstance(self._backend, InMemoryBackend):
            return self._backend.dequeue_sync(agent_id)
        return None

    async def get_queue_depth(self, agent_id: str) -> int:
        """Get number of pending messages for agent."""
        return await self._backend.get_queue_depth(agent_id)

    def get_queue_depth_sync(self, agent_id: str) -> int:
        """Synchronous version of get_queue_depth."""
        if isinstance(self._backend, InMemoryBackend):
            return self._backend.get_queue_depth_sync(agent_id)
        return 0

    async def clear_agent_queue(self, agent_id: str) -> List[Message]:
        """Clear all pending messages for agent."""
        return await self._backend.clear_queue(agent_id)

    def clear_agent_queue_sync(self, agent_id: str) -> List[Message]:
        """Synchronous version of clear_agent_queue."""
        if isinstance(self._backend, InMemoryBackend):
            return self._backend.clear_queue_sync(agent_id)
        return []
