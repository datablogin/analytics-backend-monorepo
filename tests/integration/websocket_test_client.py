"""WebSocket test client for integration testing."""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any

import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WebSocketTestClient:
    """WebSocket test client for simulating dashboard connections."""

    def __init__(self, websocket_url: str = "ws://localhost:8765"):
        self.websocket_url = websocket_url
        self.websocket = None
        self.is_connected = False
        self.received_messages = []
        self.connection_stats = {
            "messages_sent": 0,
            "messages_received": 0,
            "connection_time": None,
            "errors": 0,
        }

    async def connect(self) -> bool:
        """Connect to WebSocket server."""
        try:
            logger.info(f"Connecting to {self.websocket_url}")
            self.websocket = await websockets.connect(self.websocket_url)
            self.is_connected = True
            self.connection_stats["connection_time"] = datetime.utcnow()
            logger.info("Connected successfully")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            self.connection_stats["errors"] += 1
            return False

    async def disconnect(self):
        """Disconnect from WebSocket server."""
        if self.websocket and self.is_connected:
            await self.websocket.close()
            self.is_connected = False
            logger.info("Disconnected")

    async def send_message(self, message: dict[str, Any]):
        """Send message to WebSocket server."""
        if not self.is_connected:
            logger.error("Not connected to WebSocket server")
            return False

        try:
            message_json = json.dumps(message)
            await self.websocket.send(message_json)
            self.connection_stats["messages_sent"] += 1
            logger.info(f"Sent message: {message}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self.connection_stats["errors"] += 1
            return False

    async def receive_message(self, timeout: float = 5.0) -> dict[str, Any] | None:
        """Receive message from WebSocket server."""
        if not self.is_connected:
            logger.error("Not connected to WebSocket server")
            return None

        try:
            message = await asyncio.wait_for(self.websocket.recv(), timeout=timeout)
            message_data = json.loads(message)
            self.received_messages.append(message_data)
            self.connection_stats["messages_received"] += 1
            logger.info(f"Received message: {message_data}")
            return message_data
        except TimeoutError:
            logger.warning(f"No message received within {timeout} seconds")
            return None
        except Exception as e:
            logger.error(f"Failed to receive message: {e}")
            self.connection_stats["errors"] += 1
            return None

    async def subscribe_to_events(
        self, subscription_type: str, filters: dict[str, Any] | None = None
    ):
        """Subscribe to specific event types."""
        subscription_message = {
            "type": "subscribe",
            "subscription_type": subscription_type,
            "filters": filters or {},
        }

        success = await self.send_message(subscription_message)
        if success:
            # Wait for confirmation
            confirmation = await self.receive_message()
            return confirmation and confirmation.get("type") == "subscription_confirmed"
        return False

    async def run_performance_test(self, duration_seconds: int = 30):
        """Run performance test for specified duration."""
        logger.info(f"Starting performance test for {duration_seconds} seconds")

        if not await self.connect():
            return False

        # Subscribe to all event types
        await self.subscribe_to_events("events")
        await self.subscribe_to_events("metrics")
        await self.subscribe_to_events("alerts")

        start_time = time.time()
        message_count = 0

        try:
            while time.time() - start_time < duration_seconds:
                message = await self.receive_message(timeout=1.0)
                if message:
                    message_count += 1

                # Send periodic ping
                if int(time.time() - start_time) % 10 == 0:
                    await self.send_message({"type": "ping"})

        except KeyboardInterrupt:
            logger.info("Performance test interrupted")
        finally:
            await self.disconnect()

        # Calculate performance metrics
        elapsed_time = time.time() - start_time
        messages_per_second = message_count / elapsed_time if elapsed_time > 0 else 0

        performance_stats = {
            "duration_seconds": elapsed_time,
            "messages_received": message_count,
            "messages_per_second": messages_per_second,
            "connection_stats": self.connection_stats,
        }

        logger.info(f"Performance test results: {performance_stats}")
        return performance_stats

    def get_statistics(self) -> dict[str, Any]:
        """Get client statistics."""
        return {
            "is_connected": self.is_connected,
            "total_messages_received": len(self.received_messages),
            "connection_stats": self.connection_stats,
            "last_messages": self.received_messages[-5:]
            if self.received_messages
            else [],
        }


async def simulate_multiple_clients(
    num_clients: int = 5,
    duration_seconds: int = 60,
    websocket_url: str = "ws://localhost:8765",
):
    """Simulate multiple WebSocket clients for load testing."""
    logger.info(
        f"Starting simulation with {num_clients} clients for {duration_seconds} seconds"
    )

    clients = []
    tasks = []

    try:
        # Create and connect clients
        for i in range(num_clients):
            client = WebSocketTestClient(websocket_url)
            clients.append(client)

            # Create task for each client
            task = asyncio.create_task(
                client.run_performance_test(duration_seconds), name=f"client-{i}"
            )
            tasks.append(task)

        # Wait for all clients to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        total_messages = 0
        total_errors = 0
        successful_clients = 0

        for i, result in enumerate(results):
            if isinstance(result, dict):
                total_messages += result.get("messages_received", 0)
                total_errors += result.get("connection_stats", {}).get("errors", 0)
                successful_clients += 1
                logger.info(
                    f"Client {i}: {result['messages_received']} messages, "
                    f"{result['messages_per_second']:.2f} msg/sec"
                )
            else:
                logger.error(f"Client {i} failed: {result}")

        # Overall statistics
        overall_stats = {
            "total_clients": num_clients,
            "successful_clients": successful_clients,
            "total_messages_received": total_messages,
            "total_errors": total_errors,
            "average_messages_per_client": total_messages / successful_clients
            if successful_clients > 0
            else 0,
        }

        logger.info(f"Overall simulation results: {overall_stats}")
        return overall_stats

    finally:
        # Cleanup
        for client in clients:
            if client.is_connected:
                await client.disconnect()


async def main():
    """Main test runner."""
    websocket_url = os.getenv("WEBSOCKET_URL", "ws://localhost:8765")

    logger.info("Starting WebSocket integration tests")

    # Test 1: Single client connection test
    logger.info("=== Test 1: Single Client Connection Test ===")
    client = WebSocketTestClient(websocket_url)

    if await client.connect():
        # Subscribe to different event types
        await client.subscribe_to_events("events", {"event_type": "user_action"})
        await client.subscribe_to_events("metrics")

        # Listen for messages for 30 seconds
        for i in range(30):
            await client.receive_message(timeout=1.0)
            if i % 10 == 0:
                await client.send_message({"type": "ping", "timestamp": time.time()})

        await client.disconnect()

        stats = client.get_statistics()
        logger.info(f"Single client test results: {stats}")

    # Wait before next test
    await asyncio.sleep(5)

    # Test 2: Multiple clients simulation
    logger.info("=== Test 2: Multiple Clients Load Test ===")
    await simulate_multiple_clients(
        num_clients=3,  # Reduced for testing environment
        duration_seconds=60,
        websocket_url=websocket_url,
    )

    logger.info("WebSocket integration tests completed")


if __name__ == "__main__":
    asyncio.run(main())
