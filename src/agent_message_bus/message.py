"""Message data model, priority levels, and delivery status."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4


class MessagePriority(int, Enum):
    """Message priority levels (lower = higher priority)."""

    P0_CRITICAL = 0  # Emergency, safety-related
    P1_HIGH = 1  # Task coordination
    P2_NORMAL = 2  # Regular communication
    P3_LOW = 3  # Status updates, logs


class DeliveryStatus(str, Enum):
    """Message delivery status."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    EXPIRED = "expired"
    DEAD_LETTER = "dead_letter"


@dataclass
class Message:
    """
    Agent-to-agent message structure.

    Attributes:
        message_id: Unique identifier for this message
        from_agent: Source agent ID
        to_agent: Destination agent ID or "broadcast"
        content: Message payload dictionary
        timestamp: Creation timestamp
        priority: Message priority level
        ttl: Time-to-live in seconds (0 = no expiration)
        expires_at: Expiration timestamp (computed from ttl)
        delivery_status: Current delivery status
        delivery_attempts: Number of delivery attempts
    """

    message_id: str = field(default_factory=lambda: str(uuid4()))
    from_agent: str = field(default="")
    to_agent: str = field(default="")  # "broadcast" for broadcast messages
    content: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    priority: int = field(default=MessagePriority.P2_NORMAL.value)
    ttl: float = field(default=0.0)  # 0 means no expiration
    expires_at: Optional[datetime] = field(default=None)
    delivery_status: DeliveryStatus = field(default=DeliveryStatus.PENDING)
    delivery_attempts: int = field(default=0)
    correlation_id: Optional[str] = field(default=None)  # Links request/response pairs
    is_response: bool = field(default=False)  # True for reply/reply messages

    def __post_init__(self):
        """Compute expiration time if TTL is set."""
        if self.ttl > 0:
            self.expires_at = self.timestamp + timedelta(seconds=self.ttl)

    def is_expired(self) -> bool:
        """Check if message has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    def is_broadcast(self) -> bool:
        """Check if this is a broadcast message."""
        return self.to_agent == "broadcast"

    def __lt__(self, other: "Message") -> bool:
        """Compare for priority queue ordering (lower priority value = higher priority)."""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.timestamp < other.timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for serialization."""
        return {
            "message_id": self.message_id,
            "from_agent": self.from_agent,
            "to_agent": self.to_agent,
            "content": self.content,
            "timestamp": self.timestamp.isoformat(),
            "priority": self.priority,
            "ttl": self.ttl,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "delivery_status": self.delivery_status.value,
            "delivery_attempts": self.delivery_attempts,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """Create message from dictionary."""
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        if data.get("expires_at"):
            data["expires_at"] = datetime.fromisoformat(data["expires_at"])
        data["delivery_status"] = DeliveryStatus(data["delivery_status"])
        return cls(**data)
