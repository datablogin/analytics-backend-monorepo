"""WebSocket server for real-time dashboard updates."""

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from uuid import uuid4

import structlog
import websockets.legacy.server as websockets_legacy
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.legacy.server import WebSocketServerProtocol

from ..analytics_core.auth import AuthService, TokenData
from .config import WebSocketConfig, get_streaming_config
from .event_store import EventSchema

logger = structlog.get_logger(__name__)


class MessageType(str, Enum):
    """WebSocket message types."""

    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    DATA = "data"
    ERROR = "error"
    PING = "ping"
    PONG = "pong"
    AUTH = "auth"
    AUTH_SUCCESS = "auth_success"
    AUTH_FAILED = "auth_failed"


class SubscriptionType(str, Enum):
    """Types of real-time subscriptions."""

    EVENTS = "events"
    METRICS = "metrics"
    ALERTS = "alerts"
    PREDICTIONS = "predictions"
    AGGREGATIONS = "aggregations"
    SYSTEM_STATUS = "system_status"


@dataclass
class WebSocketMessage:
    """WebSocket message structure."""

    type: MessageType
    data: Any = None
    subscription_id: str | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Convert message to JSON string."""
        return json.dumps(
            {
                "type": self.type.value,
                "data": self.data,
                "subscription_id": self.subscription_id,
                "timestamp": self.timestamp.isoformat(),
                "metadata": self.metadata,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "WebSocketMessage":
        """Create message from JSON string."""
        data = json.loads(json_str)
        return cls(
            type=MessageType(data["type"]),
            data=data.get("data"),
            subscription_id=data.get("subscription_id"),
            timestamp=datetime.fromisoformat(
                data.get("timestamp", datetime.utcnow().isoformat())
            ),
            metadata=data.get("metadata", {}),
        )


@dataclass
class Subscription:
    """Client subscription to real-time data."""

    id: str
    client_id: str
    type: SubscriptionType
    filters: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_activity: datetime = field(default_factory=datetime.utcnow)
    message_count: int = 0

    def matches_event(self, event: EventSchema) -> bool:
        """Check if event matches subscription filters."""
        if not self.filters:
            return True

        event_dict = event.model_dump()

        for filter_key, filter_value in self.filters.items():
            # Support nested field access with dot notation
            value = event_dict
            for field_part in filter_key.split("."):
                if isinstance(value, dict) and field_part in value:
                    value = value[field_part]
                else:
                    return False

            # Check filter match
            if isinstance(filter_value, list):
                if value not in filter_value:
                    return False
            elif value != filter_value:
                return False

        return True


@dataclass
class WebSocketClient:
    """Connected WebSocket client."""

    id: str
    websocket: WebSocketServerProtocol
    subscriptions: dict[str, Subscription] = field(default_factory=dict)
    authenticated: bool = False
    connected_at: datetime = field(default_factory=datetime.utcnow)
    last_ping: datetime = field(default_factory=datetime.utcnow)
    message_count: int = 0
    last_message_time: datetime = field(default_factory=datetime.utcnow)
    message_timestamps: list[datetime] = field(default_factory=list)
    origin: str | None = None

    # Security enhancements
    token_data: TokenData | None = None
    user_id: int | None = None
    permissions: list[str] = field(default_factory=list)
    audit_session_id: str = field(default_factory=lambda: str(uuid4()))

    async def send_message(self, message: WebSocketMessage) -> bool:
        """Send message to client."""
        try:
            await self.websocket.send(message.to_json())
            self.message_count += 1
            return True
        except (ConnectionClosed, WebSocketException) as e:
            logger.debug(
                "Failed to send message to client", client_id=self.id, error=str(e)
            )
            return False

    def add_subscription(self, subscription: Subscription) -> None:
        """Add subscription for this client."""
        self.subscriptions[subscription.id] = subscription

    def remove_subscription(self, subscription_id: str) -> bool:
        """Remove subscription."""
        return self.subscriptions.pop(subscription_id, None) is not None

    def get_matching_subscriptions(
        self, subscription_type: SubscriptionType, event: EventSchema | None = None
    ) -> list[Subscription]:
        """Get subscriptions matching type and optional event."""
        matching = []

        for subscription in self.subscriptions.values():
            if subscription.type == subscription_type:
                if event is None or subscription.matches_event(event):
                    matching.append(subscription)

        return matching

    def is_rate_limited(self, rate_limit_per_minute: int) -> bool:
        """Check if client is being rate limited."""
        now = datetime.utcnow()
        minute_ago = now - timedelta(minutes=1)

        # Clean old timestamps
        self.message_timestamps = [
            ts for ts in self.message_timestamps if ts > minute_ago
        ]

        return len(self.message_timestamps) >= rate_limit_per_minute

    def record_message(self) -> None:
        """Record a new message for rate limiting."""
        now = datetime.utcnow()
        self.last_message_time = now
        self.message_timestamps.append(now)
        self.message_count += 1


class WebSocketAuthenticator:
    """Handles WebSocket authentication using JWT tokens."""

    def __init__(self, config: WebSocketConfig):
        self.config = config
        self.logger = logger.bind(component="websocket_auth")

    async def authenticate(
        self, message: WebSocketMessage
    ) -> tuple[bool, TokenData | None]:
        """Authenticate client based on JWT token in message data."""
        if not self.config.require_auth:
            return True, None

        try:
            auth_data = message.data or {}

            # Extract JWT token
            token = auth_data.get("token")
            if not token:
                self.logger.warning("Authentication failed - no token provided")
                return False, None

            # Validate JWT token using the analytics_core auth service
            try:
                token_data = await AuthService.verify_token(token)
                self.logger.info(
                    "WebSocket authentication successful",
                    user_id=token_data.user_id,
                    permissions_count=len(token_data.permissions),
                )
                return True, token_data
            except Exception as token_error:
                self.logger.warning(
                    "JWT token validation failed",
                    token=token[:20] + "..." if len(token) > 20 else token,
                    error=str(token_error),
                )
                return False, None

        except Exception as e:
            self.logger.error("Authentication error", error=str(e))
            return False, None

    def check_permissions(
        self, token_data: TokenData | None, required_permissions: list[str]
    ) -> bool:
        """Check if user has required permissions for operation."""
        if not token_data:
            return False

        # Check if user has all required permissions
        for permission in required_permissions:
            if permission not in token_data.permissions:
                self.logger.warning(
                    "Permission denied",
                    user_id=token_data.user_id,
                    required_permission=permission,
                    user_permissions=token_data.permissions,
                )
                return False
        return True


class WebSocketServer:
    """WebSocket server for real-time streaming analytics."""

    def __init__(self, config: WebSocketConfig | None = None):
        self.config = config or get_streaming_config().websocket
        self.authenticator = WebSocketAuthenticator(self.config)
        self.logger = logger.bind(component="websocket_server")

        # Client management
        self.clients: dict[str, WebSocketClient] = {}
        self.subscriptions: dict[str, Subscription] = {}

        # Server state
        self.server: websockets_legacy.WebSocketServer | None = None
        self.is_running = False

        # Background tasks
        self.ping_task: asyncio.Task | None = None
        self.cleanup_task: asyncio.Task | None = None

        # Statistics
        self.total_connections = 0
        self.total_messages_sent = 0
        self.total_messages_received = 0

        # Message handlers
        self.message_handlers: dict[MessageType, Callable] = {
            MessageType.SUBSCRIBE: self._handle_subscribe,
            MessageType.UNSUBSCRIBE: self._handle_unsubscribe,
            MessageType.PING: self._handle_ping,
            MessageType.AUTH: self._handle_auth,
        }

    async def start(self) -> None:
        """Start the WebSocket server."""
        try:
            self.server = await websockets_legacy.serve(
                self._handle_client,
                self.config.host,
                self.config.port,
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
                close_timeout=self.config.close_timeout,
                max_size=self.config.max_message_size,
                compression=self.config.compression,
            )

            self.is_running = True

            # Start background tasks
            self.ping_task = asyncio.create_task(self._ping_clients())
            self.cleanup_task = asyncio.create_task(self._cleanup_stale_connections())

            self.logger.info(
                "WebSocket server started",
                host=self.config.host,
                port=self.config.port,
                max_connections=self.config.max_connections,
            )

        except Exception as e:
            self.logger.error("Failed to start WebSocket server", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the WebSocket server."""
        self.is_running = False

        # Cancel background tasks
        if self.ping_task:
            self.ping_task.cancel()
        if self.cleanup_task:
            self.cleanup_task.cancel()

        # Close all client connections
        if self.clients:
            disconnect_tasks = [
                self._disconnect_client(client_id, "Server shutdown")
                for client_id in list(self.clients.keys())
            ]
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)

        # Close server
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        self.logger.info(
            "WebSocket server stopped",
            total_connections=self.total_connections,
            messages_sent=self.total_messages_sent,
            messages_received=self.total_messages_received,
        )

    async def _handle_client(
        self, websocket: WebSocketServerProtocol, path: str
    ) -> None:
        """Handle new client connection."""
        client_id = str(uuid4())

        # Validate origin if configured
        origin = websocket.request_headers.get("Origin")
        if not self._is_origin_allowed(origin):
            await websocket.close(code=1008, reason="Origin not allowed")
            self.logger.warning(
                "Connection rejected - invalid origin",
                client_id=client_id,
                origin=origin,
            )
            return

        # Check connection limit
        if len(self.clients) >= self.config.max_connections:
            await websocket.close(code=1013, reason="Server at capacity")
            self.logger.warning(
                "Connection rejected - server at capacity", client_id=client_id
            )
            return

        # Create client
        client = WebSocketClient(id=client_id, websocket=websocket, origin=origin)
        self.clients[client_id] = client
        self.total_connections += 1

        self.logger.info(
            "Client connected",
            client_id=client_id,
            remote_address=websocket.remote_address,
            active_clients=len(self.clients),
        )

        try:
            # Handle authentication if required
            if self.config.require_auth:
                authenticated = await self._authenticate_client(client)
                if not authenticated:
                    await websocket.close(code=1008, reason="Authentication failed")
                    return
            else:
                client.authenticated = True

            # Handle messages
            async for message_data in websocket:
                try:
                    message_str = (
                        message_data
                        if isinstance(message_data, str)
                        else message_data.decode("utf-8")
                    )
                    await self._handle_message(client, message_str)
                except Exception as e:
                    self.logger.error(
                        "Error handling message", client_id=client_id, error=str(e)
                    )

                    # Send error response
                    error_msg = WebSocketMessage(
                        type=MessageType.ERROR, data={"error": str(e)}
                    )
                    await client.send_message(error_msg)

        except ConnectionClosed:
            self.logger.info("Client disconnected normally", client_id=client_id)
        except Exception as e:
            self.logger.error(
                "Client connection error", client_id=client_id, error=str(e)
            )
        finally:
            await self._disconnect_client(client_id, "Connection closed")

    async def _authenticate_client(self, client: WebSocketClient) -> bool:
        """Authenticate a client connection using JWT tokens."""
        try:
            # Wait for authentication message
            auth_timeout = self.config.auth_timeout_seconds
            auth_data = await asyncio.wait_for(
                client.websocket.recv(), timeout=auth_timeout
            )

            auth_message = (
                auth_data if isinstance(auth_data, str) else auth_data.decode("utf-8")
            )
            message = WebSocketMessage.from_json(auth_message)

            if message.type != MessageType.AUTH:
                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.AUTH_FAILED,
                        data={"error": "Authentication required"},
                    )
                )
                self._audit_log(
                    "authentication_failed",
                    {
                        "client_id": client.id,
                        "reason": "no_auth_message",
                        "remote_address": client.websocket.remote_address,
                    },
                )
                return False

            # Authenticate using JWT
            is_authenticated, token_data = await self.authenticator.authenticate(
                message
            )

            if is_authenticated and token_data:
                client.authenticated = True
                client.token_data = token_data
                client.user_id = token_data.user_id
                client.permissions = token_data.permissions

                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.AUTH_SUCCESS,
                        data={
                            "client_id": client.id,
                            "user_id": token_data.user_id,
                            "session_id": client.audit_session_id,
                        },
                    )
                )

                self._audit_log(
                    "authentication_success",
                    {
                        "client_id": client.id,
                        "user_id": token_data.user_id,
                        "session_id": client.audit_session_id,
                        "permissions": token_data.permissions,
                        "remote_address": client.websocket.remote_address,
                    },
                )
                self.logger.info(
                    "Client authenticated",
                    client_id=client.id,
                    user_id=token_data.user_id,
                    session_id=client.audit_session_id,
                )
                return True
            else:
                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.AUTH_FAILED,
                        data={"error": "Invalid credentials"},
                    )
                )
                self._audit_log(
                    "authentication_failed",
                    {
                        "client_id": client.id,
                        "reason": "invalid_credentials",
                        "remote_address": client.websocket.remote_address,
                    },
                )
                return False

        except TimeoutError:
            self.logger.warning("Authentication timeout", client_id=client.id)
            self._audit_log(
                "authentication_timeout",
                {
                    "client_id": client.id,
                    "remote_address": client.websocket.remote_address,
                },
            )
            return False
        except Exception as e:
            self.logger.error("Authentication error", client_id=client.id, error=str(e))
            self._audit_log(
                "authentication_error",
                {
                    "client_id": client.id,
                    "error": str(e),
                    "remote_address": client.websocket.remote_address,
                },
            )
            return False

    async def _handle_message(self, client: WebSocketClient, message_str: str) -> None:
        """Handle incoming message from client."""
        try:
            # Check rate limiting
            if self.config.enable_rate_limiting and client.is_rate_limited(
                self.config.rate_limit_per_minute
            ):
                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.ERROR, data={"error": "Rate limit exceeded"}
                    )
                )
                return

            message = WebSocketMessage.from_json(message_str)
            client.record_message()
            self.total_messages_received += 1

            # Check if client is authenticated for non-auth messages
            if not client.authenticated and message.type != MessageType.AUTH:
                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.ERROR,
                        data={"error": "Authentication required"},
                    )
                )
                return

            # Handle message
            handler = self.message_handlers.get(message.type)
            if handler:
                await handler(client, message)
            else:
                self.logger.warning(
                    "Unknown message type",
                    client_id=client.id,
                    message_type=message.type,
                )

                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.ERROR,
                        data={"error": f"Unknown message type: {message.type}"},
                    )
                )

        except Exception as e:
            self.logger.error(
                "Message handling error", client_id=client.id, error=str(e)
            )
            raise

    async def _handle_subscribe(
        self, client: WebSocketClient, message: WebSocketMessage
    ) -> None:
        """Handle subscription request with permission checks."""
        try:
            sub_data = message.data or {}
            subscription_type = SubscriptionType(sub_data.get("type", "events"))
            filters = sub_data.get("filters", {})

            # Check permissions for subscription type
            required_permissions = self._get_subscription_permissions(subscription_type)
            if required_permissions and not self.authenticator.check_permissions(
                client.token_data, required_permissions
            ):
                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.ERROR,
                        data={
                            "error": f"Insufficient permissions for subscription type: {subscription_type.value}",
                            "required_permissions": required_permissions,
                        },
                    )
                )

                self._audit_log(
                    "subscription_permission_denied",
                    {
                        "client_id": client.id,
                        "user_id": client.user_id,
                        "session_id": client.audit_session_id,
                        "subscription_type": subscription_type.value,
                        "required_permissions": required_permissions,
                        "user_permissions": client.permissions,
                    },
                )
                return

            # Create subscription
            subscription = Subscription(
                id=str(uuid4()),
                client_id=client.id,
                type=subscription_type,
                filters=filters,
            )

            # Add to client and global registry
            client.add_subscription(subscription)
            self.subscriptions[subscription.id] = subscription

            # Send confirmation
            await client.send_message(
                WebSocketMessage(
                    type=MessageType.DATA,
                    data={
                        "status": "subscribed",
                        "subscription_id": subscription.id,
                        "type": subscription_type.value,
                    },
                    subscription_id=subscription.id,
                )
            )

            self._audit_log(
                "subscription_created",
                {
                    "client_id": client.id,
                    "user_id": client.user_id,
                    "session_id": client.audit_session_id,
                    "subscription_id": subscription.id,
                    "subscription_type": subscription_type.value,
                    "filters": filters,
                },
            )

            self.logger.info(
                "Client subscribed",
                client_id=client.id,
                user_id=client.user_id,
                subscription_id=subscription.id,
                subscription_type=subscription_type.value,
                filters=filters,
            )

        except Exception as e:
            await client.send_message(
                WebSocketMessage(
                    type=MessageType.ERROR,
                    data={"error": f"Subscription failed: {str(e)}"},
                )
            )

            self._audit_log(
                "subscription_error",
                {
                    "client_id": client.id,
                    "user_id": client.user_id,
                    "session_id": client.audit_session_id,
                    "error": str(e),
                },
            )
            raise

    async def _handle_unsubscribe(
        self, client: WebSocketClient, message: WebSocketMessage
    ) -> None:
        """Handle unsubscription request."""
        try:
            sub_data = message.data or {}
            subscription_id = sub_data.get("subscription_id")

            if not subscription_id:
                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.ERROR,
                        data={"error": "subscription_id required"},
                    )
                )
                return

            # Remove subscription
            removed = client.remove_subscription(subscription_id)
            if removed:
                self.subscriptions.pop(subscription_id, None)

                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.DATA,
                        data={
                            "status": "unsubscribed",
                            "subscription_id": subscription_id,
                        },
                    )
                )

                self.logger.info(
                    "Client unsubscribed",
                    client_id=client.id,
                    subscription_id=subscription_id,
                )
            else:
                await client.send_message(
                    WebSocketMessage(
                        type=MessageType.ERROR, data={"error": "Subscription not found"}
                    )
                )

        except Exception as e:
            await client.send_message(
                WebSocketMessage(
                    type=MessageType.ERROR,
                    data={"error": f"Unsubscription failed: {str(e)}"},
                )
            )
            raise

    async def _handle_ping(
        self, client: WebSocketClient, message: WebSocketMessage
    ) -> None:
        """Handle ping message."""
        client.last_ping = datetime.utcnow()
        await client.send_message(WebSocketMessage(type=MessageType.PONG))

    async def _handle_auth(
        self, client: WebSocketClient, message: WebSocketMessage
    ) -> None:
        """Handle re-authentication message."""
        # Authentication is handled in _authenticate_client
        # This handler is for re-authentication if needed
        is_authenticated, token_data = await self.authenticator.authenticate(message)

        if is_authenticated and token_data:
            # Update client authentication data
            old_user_id = client.user_id
            client.authenticated = True
            client.token_data = token_data
            client.user_id = token_data.user_id
            client.permissions = token_data.permissions

            await client.send_message(
                WebSocketMessage(
                    type=MessageType.AUTH_SUCCESS,
                    data={
                        "client_id": client.id,
                        "user_id": token_data.user_id,
                        "session_id": client.audit_session_id,
                    },
                )
            )

            self._audit_log(
                "re_authentication_success",
                {
                    "client_id": client.id,
                    "old_user_id": old_user_id,
                    "new_user_id": token_data.user_id,
                    "session_id": client.audit_session_id,
                    "permissions": token_data.permissions,
                },
            )
        else:
            await client.send_message(
                WebSocketMessage(
                    type=MessageType.AUTH_FAILED, data={"error": "Invalid credentials"}
                )
            )

            self._audit_log(
                "re_authentication_failed",
                {
                    "client_id": client.id,
                    "user_id": client.user_id,
                    "session_id": client.audit_session_id,
                },
            )

    async def _disconnect_client(self, client_id: str, reason: str) -> None:
        """Disconnect and cleanup client with audit logging."""
        client = self.clients.pop(client_id, None)
        if not client:
            return

        # Audit log disconnection
        session_duration = (datetime.utcnow() - client.connected_at).total_seconds()
        self._audit_log(
            "client_disconnected",
            {
                "client_id": client_id,
                "user_id": client.user_id,
                "session_id": client.audit_session_id,
                "reason": reason,
                "session_duration_seconds": session_duration,
                "message_count": client.message_count,
                "subscription_count": len(client.subscriptions),
                "remote_address": client.websocket.remote_address,
            },
        )

        # Remove all subscriptions
        for subscription_id in list(client.subscriptions.keys()):
            self.subscriptions.pop(subscription_id, None)

        # Close websocket if still open
        if not client.websocket.closed:
            try:
                await client.websocket.close(reason=reason)
            except Exception as e:
                self.logger.debug(
                    "Error closing websocket", client_id=client_id, error=str(e)
                )

        self.logger.info(
            "Client disconnected",
            client_id=client_id,
            user_id=client.user_id,
            reason=reason,
            active_clients=len(self.clients),
            session_duration_seconds=session_duration,
        )

    async def _ping_clients(self) -> None:
        """Periodically ping clients to keep connections alive."""
        while self.is_running:
            try:
                if self.clients:
                    ping_tasks = []
                    for client in self.clients.values():
                        if client.authenticated:
                            ping_msg = WebSocketMessage(type=MessageType.PING)
                            task = client.send_message(ping_msg)
                            ping_tasks.append(task)

                    if ping_tasks:
                        await asyncio.gather(*ping_tasks, return_exceptions=True)

                await asyncio.sleep(self.config.ping_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in ping task", error=str(e))
                await asyncio.sleep(5)

    async def _cleanup_stale_connections(self) -> None:
        """Clean up stale connections."""
        while self.is_running:
            try:
                current_time = datetime.utcnow()
                stale_clients = []

                for client_id, client in self.clients.items():
                    # Check if client hasn't pinged recently
                    time_since_ping = (current_time - client.last_ping).total_seconds()
                    if time_since_ping > (
                        self.config.ping_timeout * 3
                    ):  # 3x timeout grace period
                        stale_clients.append(client_id)

                # Disconnect stale clients
                for client_id in stale_clients:
                    await self._disconnect_client(client_id, "Stale connection")

                # Clean up stale subscriptions
                stale_subscriptions = []
                for sub_id, subscription in self.subscriptions.items():
                    if subscription.client_id not in self.clients:
                        stale_subscriptions.append(sub_id)

                for sub_id in stale_subscriptions:
                    self.subscriptions.pop(sub_id, None)

                await asyncio.sleep(60)  # Check every minute

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in cleanup task", error=str(e))
                await asyncio.sleep(10)

    # Public methods for broadcasting data

    async def broadcast_event(self, event: EventSchema) -> int:
        """Broadcast event to subscribed clients."""
        if not self.clients:
            return 0

        sent_count = 0
        broadcast_tasks = []

        for client in self.clients.values():
            if not client.authenticated:
                continue

            # Find matching subscriptions
            matching_subs = client.get_matching_subscriptions(
                SubscriptionType.EVENTS, event
            )

            for subscription in matching_subs:
                message = WebSocketMessage(
                    type=MessageType.DATA,
                    data={"event": event.model_dump(), "subscription_type": "events"},
                    subscription_id=subscription.id,
                )

                task = client.send_message(message)
                broadcast_tasks.append(task)
                subscription.message_count += 1

        # Send all messages concurrently
        if broadcast_tasks:
            results = await asyncio.gather(*broadcast_tasks, return_exceptions=True)
            sent_count = sum(1 for result in results if result is True)
            self.total_messages_sent += sent_count

        return sent_count

    async def broadcast_metrics(self, metrics: dict[str, Any]) -> int:
        """Broadcast metrics to subscribed clients."""
        if not self.clients:
            return 0

        sent_count = 0
        broadcast_tasks = []

        for client in self.clients.values():
            if not client.authenticated:
                continue

            matching_subs = client.get_matching_subscriptions(SubscriptionType.METRICS)

            for subscription in matching_subs:
                message = WebSocketMessage(
                    type=MessageType.DATA,
                    data={"metrics": metrics, "subscription_type": "metrics"},
                    subscription_id=subscription.id,
                )

                task = client.send_message(message)
                broadcast_tasks.append(task)
                subscription.message_count += 1

        if broadcast_tasks:
            results = await asyncio.gather(*broadcast_tasks, return_exceptions=True)
            sent_count = sum(1 for result in results if result is True)
            self.total_messages_sent += sent_count

        return sent_count

    async def broadcast_alert(self, alert: dict[str, Any]) -> int:
        """Broadcast alert to subscribed clients."""
        if not self.clients:
            return 0

        sent_count = 0
        broadcast_tasks = []

        for client in self.clients.values():
            if not client.authenticated:
                continue

            matching_subs = client.get_matching_subscriptions(SubscriptionType.ALERTS)

            for subscription in matching_subs:
                message = WebSocketMessage(
                    type=MessageType.DATA,
                    data={"alert": alert, "subscription_type": "alerts"},
                    subscription_id=subscription.id,
                )

                task = client.send_message(message)
                broadcast_tasks.append(task)
                subscription.message_count += 1

        if broadcast_tasks:
            results = await asyncio.gather(*broadcast_tasks, return_exceptions=True)
            sent_count = sum(1 for result in results if result is True)
            self.total_messages_sent += sent_count

        return sent_count

    def _is_origin_allowed(self, origin: str | None) -> bool:
        """Check if the origin is allowed to connect."""
        if not origin:
            return True  # Allow connections without origin header

        # Check against allowed origins list
        if "*" in self.config.allowed_origins:
            return True

        return origin in self.config.allowed_origins

    def _get_subscription_permissions(
        self, subscription_type: SubscriptionType
    ) -> list[str]:
        """Get required permissions for subscription type."""
        permission_map = {
            SubscriptionType.EVENTS: ["streaming.events.read"],
            SubscriptionType.METRICS: ["streaming.metrics.read"],
            SubscriptionType.ALERTS: ["streaming.alerts.read"],
            SubscriptionType.PREDICTIONS: [
                "streaming.predictions.read",
                "ml.inference.read",
            ],
            SubscriptionType.AGGREGATIONS: ["streaming.aggregations.read"],
            SubscriptionType.SYSTEM_STATUS: ["streaming.admin.read"],
        }
        return permission_map.get(subscription_type, [])

    def _audit_log(self, event_type: str, data: dict[str, Any]) -> None:
        """Log security-relevant events for audit purposes."""
        audit_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "component": "websocket_server",
            "server_host": self.config.host,
            "server_port": self.config.port,
            **data,
        }

        # Use structured logging for audit events
        self.logger.info(
            "WebSocket audit event",
            audit_event=audit_entry,
            event_type=event_type,
        )

    def get_stats(self) -> dict[str, Any]:
        """Get WebSocket server statistics."""
        return {
            "is_running": self.is_running,
            "active_clients": len(self.clients),
            "total_connections": self.total_connections,
            "total_subscriptions": len(self.subscriptions),
            "messages_sent": self.total_messages_sent,
            "messages_received": self.total_messages_received,
            "config": {
                "host": self.config.host,
                "port": self.config.port,
                "max_connections": self.config.max_connections,
                "require_auth": self.config.require_auth,
            },
        }


# Global WebSocket server instance
_websocket_server: WebSocketServer | None = None


async def get_websocket_server() -> WebSocketServer:
    """Get the global WebSocket server instance."""
    global _websocket_server

    if _websocket_server is None:
        _websocket_server = WebSocketServer()
        await _websocket_server.start()

    return _websocket_server


async def shutdown_websocket_server() -> None:
    """Shutdown the global WebSocket server."""
    global _websocket_server

    if _websocket_server:
        await _websocket_server.stop()
        _websocket_server = None
