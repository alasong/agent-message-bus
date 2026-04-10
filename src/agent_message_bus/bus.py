"""Message bus with registration, broadcast, delivery confirmation, and dead letter queue."""

import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

from agent_message_bus.backend import Backend, InMemoryBackend
from agent_message_bus.message import DeliveryStatus, Message, MessagePriority
from agent_message_bus.router import MessageRouter


class MessageBus:
    """
    Agent-to-agent message bus with priority-based routing.

    Features:
    - Agent registration/unregistration
    - Point-to-point and broadcast messaging
    - Priority-ordered message queues
    - Message TTL with automatic expiration
    - Dead letter queue for failed deliveries
    - Delivery confirmation tracking
    - Async-first with sync compatibility

    Example:
        >>> bus = MessageBus()
        >>> bus.register_agent("agent_1")
        >>> bus.register_agent("agent_2")
        >>> await bus.send("agent_1", "agent_2", {"task": "compute"})
        >>> msg = await bus.receive("agent_2", timeout=5.0)
    """

    def __init__(
        self,
        backend: Optional[Backend] = None,
        max_delivery_attempts: int = 3,
        default_ttl: float = 60.0,
    ):
        """
        Initialize the message bus.

        Args:
            backend: Queue backend instance. Defaults to InMemoryBackend.
            max_delivery_attempts: Maximum delivery attempts before dead letter
            default_ttl: Default message TTL in seconds
        """
        self._backend = backend or InMemoryBackend()
        self._registered_agents: Dict[str, bool] = {}
        self._agent_metadata: Dict[str, Dict[str, Any]] = {}
        self._router = MessageRouter(backend=self._backend)
        self._dead_letter_queue: List[Message] = []
        self._max_delivery_attempts = max_delivery_attempts
        self._default_ttl = default_ttl
        self._lock = asyncio.Lock()
        self._message_events: Dict[str, asyncio.Event] = defaultdict(asyncio.Event)
        self._delivery_confirmations: Dict[str, Dict[str, bool]] = defaultdict(dict)
        self._response_channels: Dict[str, asyncio.Queue] = {}
        self._orphaned_responses: Dict[str, List[Message]] = defaultdict(list)
        # correlation_ids where a request was sent (request messages should NOT go to response channel)
        self._active_request_ids: set = set()

    @property
    def backend(self) -> Backend:
        """Access the underlying backend instance."""
        return self._backend

    async def initialize(self) -> None:
        """Initialize backend resources."""
        await self._backend.initialize()

    async def close(self) -> None:
        """Close backend resources."""
        await self._backend.close()

    # --- Agent Lifecycle ---

    def register_agent(self, agent_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Register an agent for messaging.

        Args:
            agent_id: Unique identifier for the agent
            metadata: Optional metadata dictionary (defaults to empty dict)
        """
        self._registered_agents[agent_id] = True
        self._agent_metadata[agent_id] = metadata if metadata is not None else {}
        self._router.set_agent_availability_sync(agent_id, True)

    async def register_agent_async(
        self, agent_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Async version of agent registration.

        Args:
            agent_id: Unique identifier for the agent
            metadata: Optional metadata dictionary (defaults to empty dict)
        """
        async with self._lock:
            self._registered_agents[agent_id] = True
            self._agent_metadata[agent_id] = metadata if metadata is not None else {}
        await self._router.set_agent_availability(agent_id, True)

    def unregister_agent(self, agent_id: str) -> None:
        """Unregister an agent, moving pending messages to dead letter queue."""
        self._registered_agents.pop(agent_id, None)
        self._agent_metadata.pop(agent_id, None)
        self._router.set_agent_availability_sync(agent_id, False)
        pending = self._router.clear_agent_queue_sync(agent_id)
        for msg in pending:
            msg.delivery_status = DeliveryStatus.DEAD_LETTER
            self._dead_letter_queue.append(msg)

    async def unregister_agent_async(self, agent_id: str) -> None:
        """Async version of agent unregistration."""
        async with self._lock:
            self._registered_agents.pop(agent_id, None)
            self._agent_metadata.pop(agent_id, None)
        await self._router.set_agent_availability(agent_id, False)
        pending = await self._router.clear_agent_queue(agent_id)
        for msg in pending:
            msg.delivery_status = DeliveryStatus.DEAD_LETTER
            self._dead_letter_queue.append(msg)

    def is_registered(self, agent_id: str) -> bool:
        """Check if agent is registered."""
        return agent_id in self._registered_agents

    def get_agent_metadata(self, agent_id: str) -> Dict[str, Any]:
        """Get metadata for a registered agent. Returns empty dict if not found."""
        return self._agent_metadata.get(agent_id, {})

    def list_agents(
        self, include_metadata: bool = False
    ) -> Union[List[str], Dict[str, Dict[str, Any]]]:
        """List all registered agents.

        Args:
            include_metadata: If True, return dict with metadata. If False, return list of IDs.

        Returns:
            List of agent IDs or dict mapping agent IDs to their metadata.
        """
        if include_metadata:
            return dict(self._agent_metadata)
        return list(self._registered_agents.keys())

    # --- Sending ---

    async def send(
        self,
        from_agent: str,
        to_agent: str,
        message: dict,
        priority: int = MessagePriority.P2_NORMAL.value,
        ttl: Optional[float] = None,
        require_confirmation: bool = False,
    ) -> str:
        """
        Send a point-to-point message.

        Args:
            from_agent: Source agent ID
            to_agent: Target agent ID
            message: Message content dictionary
            priority: Message priority level
            ttl: Time-to-live in seconds (None = use default)
            require_confirmation: Whether to require delivery confirmation

        Returns:
            Message ID for tracking

        Raises:
            ValueError: If source or target agent not registered
        """
        if not self.is_registered(from_agent):
            raise ValueError(f"Source agent '{from_agent}' not registered")
        if not self.is_registered(to_agent):
            raise ValueError(f"Target agent '{to_agent}' not registered")

        msg_ttl = ttl if ttl is not None else self._default_ttl
        msg = Message(
            from_agent=from_agent,
            to_agent=to_agent,
            content=message,
            priority=priority,
            ttl=msg_ttl,
        )

        await self._router.enqueue_for_agent(to_agent, msg, event=self._message_events[to_agent])

        if require_confirmation:
            self._delivery_confirmations[to_agent][msg.message_id] = False

        return msg.message_id

    async def broadcast(
        self,
        from_agent: str,
        message: dict,
        priority: int = MessagePriority.P2_NORMAL.value,
        ttl: Optional[float] = None,
        require_confirmation: bool = False,
    ) -> str:
        """
        Broadcast a message to all registered agents.

        Args:
            from_agent: Source agent ID
            message: Message content dictionary
            priority: Message priority level
            ttl: Time-to-live in seconds

        Returns:
            Message ID for tracking

        Raises:
            ValueError: If source agent not registered
        """
        if not self.is_registered(from_agent):
            raise ValueError(f"Source agent '{from_agent}' not registered")

        msg_ttl = ttl if ttl is not None else self._default_ttl
        msg = Message(
            from_agent=from_agent,
            to_agent="broadcast",
            content=message,
            priority=priority,
            ttl=msg_ttl,
        )

        targets = await self._router.route_message(msg, list(self._registered_agents.keys()))
        first_msg_id: Optional[str] = None
        for target in targets:
            # Each broadcast target gets a unique copy of the message
            target_msg = Message(
                from_agent=from_agent,
                to_agent=target,
                content=message,
                priority=priority,
                ttl=msg_ttl,
            )
            if first_msg_id is None:
                first_msg_id = target_msg.message_id
            await self._router.enqueue_for_agent(target, target_msg, event=self._message_events[target])
            if require_confirmation:
                self._delivery_confirmations[target][target_msg.message_id] = False
        return first_msg_id

    # --- Receiving ---

    async def receive(self, agent_id: str, timeout: float = 0.0) -> Optional[Message]:
        """
        Receive a message for an agent.

        Args:
            agent_id: Agent ID to receive for
            timeout: Timeout in seconds (0 = immediate, no wait)

        Returns:
            Message if available, None if timeout or queue empty
        """
        if not self.is_registered(agent_id):
            raise ValueError(f"Agent '{agent_id}' not registered")

        msg = await self._router.get_next_for_agent(agent_id)
        if msg is not None:
            msg.delivery_status = DeliveryStatus.DELIVERED
            # Route response messages (only!) to their dedicated channel
            if msg.is_response and msg.correlation_id and msg.correlation_id in self._response_channels:
                await self._response_channels[msg.correlation_id].put(msg)
                return msg
            return msg

        if timeout > 0:
            try:
                await asyncio.wait_for(self._message_events[agent_id].wait(), timeout=timeout)
                self._message_events[agent_id].clear()
                msg = await self._router.get_next_for_agent(agent_id)
                if msg is not None:
                    msg.delivery_status = DeliveryStatus.DELIVERED
                return msg
            except asyncio.TimeoutError:
                return None

        return None

    # --- Request / Response ---

    async def request(
        self,
        from_agent: str,
        to_agent: str,
        content: dict,
        priority: int = MessagePriority.P2_NORMAL.value,
        ttl: Optional[float] = None,
        timeout: float = 30.0,
    ) -> str:
        """
        Send a request and create a response channel for awaiting the reply.

        Args:
            from_agent: Requesting agent ID
            to_agent: Target agent ID
            content: Request payload
            priority: Message priority level
            ttl: Request message TTL
            timeout: Max seconds to wait for response

        Returns:
            Request ID (used to correlate the response)

        Raises:
            ValueError: If either agent not registered
        """
        if not self.is_registered(from_agent):
            raise ValueError(f"Source agent '{from_agent}' not registered")
        if not self.is_registered(to_agent):
            raise ValueError(f"Target agent '{to_agent}' not registered")

        req_id = str(uuid4())
        req_msg = Message(
            from_agent=from_agent,
            to_agent=to_agent,
            content=content,
            priority=priority,
            ttl=ttl if ttl is not None else self._default_ttl,
            correlation_id=req_id,
        )
        self._active_request_ids.add(req_id)

        self._response_channels[req_id] = asyncio.Queue()
        await self._router.enqueue_for_agent(to_agent, req_msg, event=self._message_events[to_agent])
        return req_id

    async def reply(
        self,
        from_agent: str,
        to_agent: str,
        request_id: str,
        content: dict,
        priority: int = MessagePriority.P2_NORMAL.value,
    ) -> None:
        """
        Send a reply to a request.

        Args:
            from_agent: Replying agent ID
            to_agent: Original requester agent ID
            request_id: The request ID from the original request
            content: Reply payload
            priority: Reply message priority
        """
        resp_msg = Message(
            from_agent=from_agent,
            to_agent=to_agent,
            content=content,
            priority=priority,
            correlation_id=request_id,
            is_response=True,
        )
        await self._router.enqueue_for_agent(to_agent, resp_msg, event=self._message_events[to_agent])

    async def receive_response(
        self, agent_id: str, request_id: str, timeout: float = 30.0
    ) -> Optional[Message]:
        """
        Wait for and receive the response to a specific request.

        Checks the dedicated response channel and also drains the agent's
        regular queue looking for the matching correlation_id.

        Args:
            agent_id: The agent that sent the original request
            request_id: The request ID returned by request()
            timeout: Max seconds to wait

        Returns:
            Response message or None on timeout
        """
        channel = self._response_channels.get(request_id)
        if channel is None:
            raise ValueError(f"No response channel for request '{request_id}'")

        end = asyncio.get_event_loop().time() + timeout
        while True:
            # Check dedicated channel first
            if not channel.empty():
                return channel.get_nowait()

            remaining = end - asyncio.get_event_loop().time()
            if remaining <= 0:
                return None

            # Drain agent's queue looking for the matching response
            msg = await self._router.get_next_for_agent(agent_id)
            if msg is not None:
                msg.delivery_status = DeliveryStatus.DELIVERED
                if msg.is_response and msg.correlation_id and msg.correlation_id in self._response_channels:
                    await self._response_channels[msg.correlation_id].put(msg)
                else:
                    # Not a response — stash it so it's not lost
                    self._orphaned_responses[agent_id].append(msg)

                # After draining, check the channel again
                if not channel.empty():
                    return channel.get_nowait()
            else:
                try:
                    await asyncio.wait_for(
                        self._message_events[agent_id].wait(), timeout=remaining
                    )
                    self._message_events[agent_id].clear()
                except asyncio.TimeoutError:
                    return None

    # --- Queue Info ---

    def get_pending_count(self, agent_id: str) -> int:
        """Get number of pending messages for an agent."""
        return self._router.get_queue_depth_sync(agent_id)

    async def get_pending_count_async(self, agent_id: str) -> int:
        """Async version of get_pending_count."""
        return await self._router.get_queue_depth(agent_id)

    # --- Dead Letter Queue ---

    def get_dead_letter_queue(self) -> List[Message]:
        """Get all messages in dead letter queue."""
        return self._dead_letter_queue.copy()

    def clear_dead_letter_queue(self) -> int:
        """Clear the dead letter queue. Returns count of cleared messages."""
        count = len(self._dead_letter_queue)
        self._dead_letter_queue.clear()
        return count

    # --- Delivery Confirmation ---

    async def confirm_delivery(self, agent_id: str, message_id: str) -> bool:
        """Confirm delivery of a message."""
        async with self._lock:
            if agent_id in self._delivery_confirmations:
                if message_id in self._delivery_confirmations[agent_id]:
                    self._delivery_confirmations[agent_id][message_id] = True
                    return True
        return False

    async def check_delivery_confirmation(self, message_id: str) -> Optional[bool]:
        """Check if a message delivery was confirmed."""
        async with self._lock:
            for agent_id, confirmations in self._delivery_confirmations.items():
                if message_id in confirmations:
                    return confirmations[message_id]
        return None

    # --- Error Handling ---

    async def handle_failed_delivery(self, message: Message) -> None:
        """Handle a failed delivery attempt with retry and dead letter logic."""
        message.delivery_attempts += 1
        if message.delivery_attempts >= self._max_delivery_attempts:
            message.delivery_status = DeliveryStatus.DEAD_LETTER
            self._dead_letter_queue.append(message)
        else:
            message.delivery_status = DeliveryStatus.PENDING
            if self.is_registered(message.to_agent):
                await self._router.enqueue_for_agent(message.to_agent, message)

    # --- Statistics ---

    def get_stats(self) -> Dict[str, Any]:
        """Get message bus statistics."""
        total_pending = sum(
            self._router.get_queue_depth_sync(a) for a in self._registered_agents.keys()
        )
        return {
            "registered_agents": len(self._registered_agents),
            "total_pending_messages": total_pending,
            "dead_letter_count": len(self._dead_letter_queue),
            "max_delivery_attempts": self._max_delivery_attempts,
            "default_ttl": self._default_ttl,
        }
