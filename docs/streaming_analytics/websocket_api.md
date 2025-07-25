# WebSocket API Documentation

The streaming analytics WebSocket API provides real-time access to events, metrics, and ML predictions through persistent connections.

## Overview

The WebSocket server is implemented in `libs/streaming_analytics/websocket_server.py` and provides:

- Real-time event streaming
- Live metrics broadcasting
- Alert notifications
- ML prediction results
- System status updates
- Client authentication and rate limiting

## Connection

### Basic Connection

```javascript
const ws = new WebSocket('ws://localhost:8765');
```

### With Authentication

```javascript
const ws = new WebSocket('ws://localhost:8765');
ws.onopen = function() {
    ws.send(JSON.stringify({
        type: 'auth',
        data: {
            token: 'your-auth-token'
        }
    }));
};
```

## Message Format

All WebSocket messages use JSON with the following structure:

```json
{
    "type": "message_type",
    "data": {},
    "subscription_id": "optional-subscription-id",
    "timestamp": "2024-01-01T00:00:00.000Z",
    "metadata": {}
}
```

## Message Types

### Client to Server Messages

#### AUTH
Authenticate the client connection.

```json
{
    "type": "auth",
    "data": {
        "token": "your-auth-token"
    }
}
```

**Response:**
- `auth_success`: Authentication successful
- `auth_failed`: Authentication failed

#### SUBSCRIBE
Subscribe to real-time data streams.

```json
{
    "type": "subscribe",
    "data": {
        "type": "events|metrics|alerts|predictions|aggregations|system_status",
        "filters": {
            "event_type": "user_action",
            "user_id": ["user123", "user456"]
        }
    }
}
```

**Subscription Types:**
- `events`: Raw events from the event store
- `metrics`: System and application metrics
- `alerts`: Alert notifications
- `predictions`: ML prediction results
- `aggregations`: Windowed aggregations
- `system_status`: System health and status

**Filters:**
- Dot notation supported: `"user.id": "123"`
- Array values for multiple matches: `"event_type": ["login", "purchase"]`
- Exact matches only (no regex support)

#### UNSUBSCRIBE
Cancel an existing subscription.

```json
{
    "type": "unsubscribe",
    "data": {
        "subscription_id": "subscription-uuid"
    }
}
```

#### PING
Send keepalive ping to server.

```json
{
    "type": "ping"
}
```

### Server to Client Messages

#### AUTH_SUCCESS
Authentication successful.

```json
{
    "type": "auth_success",
    "data": {
        "client_id": "client-uuid"
    }
}
```

#### AUTH_FAILED
Authentication failed.

```json
{
    "type": "auth_failed",
    "data": {
        "error": "Invalid credentials"
    }
}
```

#### DATA
Real-time data delivery.

```json
{
    "type": "data",
    "data": {
        "event": {
            "event_id": "evt_123",
            "event_type": "user_action",
            "user_id": "user_456",
            "timestamp": "2024-01-01T00:00:00Z",
            "payload": {}
        },
        "subscription_type": "events"
    },
    "subscription_id": "sub_789"
}
```

#### ERROR
Error messages.

```json
{
    "type": "error",
    "data": {
        "error": "Rate limit exceeded"
    }
}
```

#### PONG
Response to ping.

```json
{
    "type": "pong"
}
```

## Configuration

WebSocket server configuration is defined in `libs/streaming_analytics/config.py`:

```python
class WebSocketConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8765
    max_connections: int = 1000
    ping_interval: int = 20
    ping_timeout: int = 10
    close_timeout: int = 10
    max_message_size: int = 1024 * 1024  # 1MB
    compression: str | None = "deflate"
    require_auth: bool = True
    auth_timeout_seconds: int = 30
    allowed_origins: list[str] = ["*"]
    enable_rate_limiting: bool = True
    rate_limit_per_minute: int = 60
```

### Environment Variables

Configure using environment variables with `STREAMING_WEBSOCKET__` prefix:

```bash
STREAMING_WEBSOCKET__HOST=0.0.0.0
STREAMING_WEBSOCKET__PORT=8765
STREAMING_WEBSOCKET__MAX_CONNECTIONS=1000
STREAMING_WEBSOCKET__REQUIRE_AUTH=true
STREAMING_WEBSOCKET__RATE_LIMIT_PER_MINUTE=60
```

## Rate Limiting

The server implements per-client rate limiting:

- Default: 60 messages per minute per client
- Configurable via `rate_limit_per_minute`
- Returns error message when exceeded
- Automatically cleans up old message timestamps

## Security Features

### Authentication
- Token-based authentication
- Configurable timeout for auth messages
- Re-authentication support

### Origin Validation
- Configurable allowed origins
- Wildcard support (`*`)
- Automatic origin header checking

### Connection Limits
- Maximum concurrent connections
- Graceful rejection when at capacity
- Connection cleanup and monitoring

## Client Examples

### JavaScript Client

```javascript
class StreamingClient {
    constructor(url, token) {
        this.url = url;
        this.token = token;
        this.subscriptions = new Map();
        this.connect();
    }
    
    connect() {
        this.ws = new WebSocket(this.url);
        
        this.ws.onopen = () => {
            console.log('Connected to streaming server');
            this.authenticate();
        };
        
        this.ws.onmessage = (event) => {
            const message = JSON.parse(event.data);
            this.handleMessage(message);
        };
        
        this.ws.onclose = () => {
            console.log('Disconnected from streaming server');
            // Implement reconnection logic
        };
    }
    
    authenticate() {
        this.send({
            type: 'auth',
            data: { token: this.token }
        });
    }
    
    subscribe(type, filters = {}) {
        const subscriptionId = this.generateId();
        this.send({
            type: 'subscribe',
            data: { type, filters }
        });
        return subscriptionId;
    }
    
    unsubscribe(subscriptionId) {
        this.send({
            type: 'unsubscribe',
            data: { subscription_id: subscriptionId }
        });
        this.subscriptions.delete(subscriptionId);
    }
    
    send(message) {
        if (this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(message));
        }
    }
    
    handleMessage(message) {
        switch (message.type) {
            case 'auth_success':
                console.log('Authentication successful');
                break;
            case 'data':
                this.handleData(message);
                break;
            case 'error':
                console.error('Server error:', message.data.error);
                break;
        }
    }
    
    handleData(message) {
        const callback = this.subscriptions.get(message.subscription_id);
        if (callback) {
            callback(message.data);
        }
    }
    
    generateId() {
        return Math.random().toString(36).substr(2, 9);
    }
}

// Usage
const client = new StreamingClient('ws://localhost:8765', 'your-token');

// Subscribe to user events
client.subscribe('events', {
    'event_type': 'user_action',
    'payload.user_id': 'user123'
});

// Subscribe to metrics
client.subscribe('metrics');
```

### Python Client

```python
import asyncio
import json
import websockets

class StreamingClient:
    def __init__(self, uri, token):
        self.uri = uri
        self.token = token
        self.subscriptions = {}
        
    async def connect(self):
        async with websockets.connect(self.uri) as websocket:
            self.websocket = websocket
            
            # Authenticate
            await self.send({
                'type': 'auth',
                'data': {'token': self.token}
            })
            
            # Handle messages
            async for message in websocket:
                await self.handle_message(json.loads(message))
    
    async def send(self, message):
        await self.websocket.send(json.dumps(message))
    
    async def subscribe(self, sub_type, filters=None):
        await self.send({
            'type': 'subscribe',
            'data': {
                'type': sub_type,
                'filters': filters or {}
            }
        })
    
    async def handle_message(self, message):
        msg_type = message.get('type')
        
        if msg_type == 'auth_success':
            print('Authentication successful')
        elif msg_type == 'data':
            print(f"Received data: {message['data']}")
        elif msg_type == 'error':
            print(f"Error: {message['data']['error']}")

# Usage
async def main():
    client = StreamingClient('ws://localhost:8765', 'your-token')
    await client.connect()

asyncio.run(main())
```

## Performance Considerations

### Connection Management
- The server automatically pings clients every 20 seconds
- Stale connections are cleaned up automatically
- Connection limits prevent resource exhaustion

### Message Broadcasting
- Uses asyncio.gather for concurrent message delivery
- Filters are applied efficiently before sending
- Failed deliveries are handled gracefully

### Memory Management
- Message timestamps are cleaned automatically
- Stale subscriptions are removed
- Client state is cleaned on disconnect

## Monitoring and Metrics

The WebSocket server provides statistics via the `get_stats()` method:

```python
{
    "is_running": True,
    "active_clients": 15,
    "total_connections": 150,
    "total_subscriptions": 25,
    "messages_sent": 5000,
    "messages_received": 1200,
    "config": {
        "host": "0.0.0.0",
        "port": 8765,
        "max_connections": 1000,
        "require_auth": True
    }
}
```

## Error Handling

Common error scenarios and responses:

| Error | Response |
|-------|----------|
| Authentication timeout | Connection closed with code 1008 |
| Invalid origin | Connection closed with code 1008 |
| Server at capacity | Connection closed with code 1013 |
| Rate limit exceeded | Error message: "Rate limit exceeded" |
| Invalid message format | Error message with details |
| Unknown message type | Error message: "Unknown message type" |

## Integration with Services

### Starting the WebSocket Server

```python
from libs.streaming_analytics import WebSocketServer, get_streaming_config

config = get_streaming_config()
server = WebSocketServer(config.websocket)

# Start server
await server.start()

# Broadcast events
await server.broadcast_event(event)

# Broadcast metrics
await server.broadcast_metrics(metrics_dict)

# Stop server
await server.stop()
```

### Global Server Instance

```python
from libs.streaming_analytics.websocket_server import get_websocket_server

# Get singleton instance
server = await get_websocket_server()

# Use server for broadcasting
await server.broadcast_event(event)
```