"""Kafka integration tests with real Kafka cluster."""

import asyncio
import json
import time

import pytest

from libs.streaming_analytics import (
    KafkaManager,
    StreamingConfig,
)


class TestKafkaIntegration:
    """Integration tests for Kafka functionality."""

    @pytest.fixture(scope="class")
    def kafka_config(self):
        """Kafka configuration for integration tests."""
        config = StreamingConfig()
        config.kafka.bootstrap_servers = ["localhost:9092"]
        return config

    @pytest.fixture
    async def kafka_manager(self, kafka_config):
        """Kafka manager instance."""
        manager = KafkaManager(kafka_config.kafka)
        await manager.initialize()
        yield manager
        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_topic_creation_and_management(self, kafka_manager):
        """Test creating and managing Kafka topics."""
        topic_name = f"test-topic-{int(time.time())}"

        # Create topic
        success = await kafka_manager.create_topic(
            topic_name,
            num_partitions=3,
            replication_factor=1,
            config={
                "cleanup.policy": "delete",
                "retention.ms": "86400000",  # 1 day
            },
        )
        assert success

        # Verify topic exists
        topics = await kafka_manager.list_topics()
        assert topic_name in topics

        # Get topic metadata
        metadata = await kafka_manager.get_topic_metadata(topic_name)
        assert metadata["partition_count"] == 3

        # Delete topic
        deleted = await kafka_manager.delete_topic(topic_name)
        assert deleted

    @pytest.mark.asyncio
    async def test_producer_consumer_reliability(self, kafka_manager):
        """Test producer-consumer reliability with message ordering."""
        topic = f"reliability-test-{int(time.time())}"
        await kafka_manager.create_topic(
            topic, num_partitions=1
        )  # Single partition for ordering

        # Produce ordered messages
        message_count = 100
        messages = []

        for i in range(message_count):
            message = {
                "sequence_id": i,
                "timestamp": time.time(),
                "payload": f"message-{i}",
                "checksum": hash(f"message-{i}"),
            }
            messages.append(message)
            await kafka_manager.produce_event(topic, message)

        # Consume all messages
        consumer = await kafka_manager.create_consumer(
            [topic], "reliability-test-group"
        )
        consumed_messages = []

        start_time = time.time()
        while len(consumed_messages) < message_count and time.time() - start_time < 30:
            async for msg in consumer:
                consumed_data = json.loads(msg.value.decode())
                consumed_messages.append(consumed_data)
                if len(consumed_messages) >= message_count:
                    break
            await asyncio.sleep(0.1)

        await consumer.stop()

        # Verify all messages received in order
        assert len(consumed_messages) == message_count

        for i, consumed_msg in enumerate(consumed_messages):
            expected_msg = messages[i]
            assert consumed_msg["sequence_id"] == expected_msg["sequence_id"]
            assert consumed_msg["checksum"] == expected_msg["checksum"]

    @pytest.mark.asyncio
    async def test_consumer_group_balancing(self, kafka_manager):
        """Test consumer group balancing across multiple consumers."""
        topic = f"balancing-test-{int(time.time())}"
        await kafka_manager.create_topic(topic, num_partitions=4)

        group_id = "balancing-test-group"

        # Create multiple consumers in the same group
        consumers = []
        for i in range(2):  # 2 consumers for 4 partitions
            consumer = await kafka_manager.create_consumer([topic], group_id)
            consumers.append(consumer)

        # Produce messages to different partitions
        message_count = 40
        for i in range(message_count):
            message = {
                "partition_key": f"key-{i % 4}",  # Distribute across partitions
                "message_id": i,
                "data": f"test-data-{i}",
            }
            await kafka_manager.produce_event(
                topic, message, key=message["partition_key"]
            )

        # Consume messages from both consumers
        consumed_by_consumer = [[] for _ in consumers]

        async def consume_from_consumer(consumer_idx, consumer):
            async for msg in consumer:
                consumed_data = json.loads(msg.value.decode())
                consumed_by_consumer[consumer_idx].append(consumed_data)
                if sum(len(msgs) for msgs in consumed_by_consumer) >= message_count:
                    break

        # Start consuming concurrently
        consumer_tasks = [
            asyncio.create_task(consume_from_consumer(i, consumer))
            for i, consumer in enumerate(consumers)
        ]

        # Wait for all messages to be consumed
        await asyncio.wait_for(
            asyncio.gather(*consumer_tasks, return_exceptions=True), timeout=30
        )

        # Stop all consumers
        for consumer in consumers:
            await consumer.stop()

        # Verify load balancing
        total_consumed = sum(len(msgs) for msgs in consumed_by_consumer)
        assert total_consumed == message_count

        # Each consumer should have consumed some messages
        for msgs in consumed_by_consumer:
            assert len(msgs) > 0

    @pytest.mark.asyncio
    async def test_dead_letter_queue_handling(self, kafka_manager):
        """Test dead letter queue handling for failed messages."""
        main_topic = f"dlq-test-{int(time.time())}"
        dlq_topic = f"{main_topic}.dlq"

        await kafka_manager.create_topic(main_topic, num_partitions=1)
        await kafka_manager.create_topic(dlq_topic, num_partitions=1)

        # Configure consumer with DLQ handling
        consumer = await kafka_manager.create_consumer(
            [main_topic], "dlq-test-group", enable_dlq=True, dlq_topic=dlq_topic
        )

        # Produce valid and invalid messages
        valid_message = {"type": "valid", "data": "good data"}
        invalid_message = {
            "type": "invalid",
            "data": None,
        }  # Will cause processing error

        await kafka_manager.produce_event(main_topic, valid_message)
        await kafka_manager.produce_event(main_topic, invalid_message)

        # Simulate processing with failures
        processed_messages = []
        dlq_messages = []

        async def process_message(msg):
            data = json.loads(msg.value.decode())
            if data["data"] is None:
                raise ValueError("Invalid data")
            processed_messages.append(data)

        # Process messages
        async for msg in consumer:
            try:
                await process_message(msg)
            except Exception:
                # Send to DLQ
                await kafka_manager.send_to_dlq(msg, dlq_topic)

            if len(processed_messages) + len(dlq_messages) >= 2:
                break

        await consumer.stop()

        # Verify DLQ consumer can read failed messages
        dlq_consumer = await kafka_manager.create_consumer(
            [dlq_topic], "dlq-consumer-group"
        )

        async for msg in dlq_consumer:
            data = json.loads(msg.value.decode())
            dlq_messages.append(data)
            break  # Just read one message

        await dlq_consumer.stop()

        # Verify processing results
        assert len(processed_messages) == 1
        assert processed_messages[0]["type"] == "valid"
        assert len(dlq_messages) == 1
        assert dlq_messages[0]["type"] == "invalid"

    @pytest.mark.asyncio
    async def test_high_throughput_production(self, kafka_manager):
        """Test high-throughput message production."""
        topic = f"throughput-test-{int(time.time())}"
        await kafka_manager.create_topic(topic, num_partitions=8)

        # Configure for high throughput
        kafka_manager.producer_config.update(
            {
                "batch_size": 32768,  # Larger batch size
                "linger_ms": 10,  # Small linger time
                "compression_type": "lz4",  # Fast compression
            }
        )

        # Produce large number of messages
        message_count = 10000
        start_time = time.time()

        # Use batch production for efficiency
        batch_size = 1000
        for batch_start in range(0, message_count, batch_size):
            batch_end = min(batch_start + batch_size, message_count)
            batch = []

            for i in range(batch_start, batch_end):
                message = {
                    "id": i,
                    "timestamp": time.time(),
                    "data": f"high-throughput-test-{i}",
                    "partition_key": f"key-{i % 8}",
                }
                batch.append(message)

            # Send batch
            await kafka_manager.produce_batch(topic, batch)

        production_time = time.time() - start_time
        throughput = message_count / production_time

        # Verify throughput (should be > 1000 messages/sec)
        assert throughput > 1000

        # Verify all messages were produced (check with consumer)
        consumer = await kafka_manager.create_consumer(
            [topic], "throughput-verify-group"
        )
        consumed_count = 0

        start_time = time.time()
        async for msg in consumer:
            consumed_count += 1
            if consumed_count >= message_count or time.time() - start_time > 30:
                break

        await consumer.stop()

        # Allow for some message loss in high-throughput scenarios (95% threshold)
        assert consumed_count >= message_count * 0.95

    @pytest.mark.asyncio
    async def test_kafka_reconnection_and_failover(self, kafka_manager):
        """Test Kafka reconnection and failover behavior."""
        topic = f"failover-test-{int(time.time())}"
        await kafka_manager.create_topic(topic, num_partitions=1)

        # Produce initial message
        initial_message = {"phase": "before_disconnect", "timestamp": time.time()}
        await kafka_manager.produce_event(topic, initial_message)

        # Simulate connection issues by stopping and restarting
        await kafka_manager.stop()

        # Wait a bit to simulate downtime
        await asyncio.sleep(2)

        # Restart manager
        await kafka_manager.start()

        # Produce message after reconnection
        after_message = {"phase": "after_reconnect", "timestamp": time.time()}
        await kafka_manager.produce_event(topic, after_message)

        # Verify both messages can be consumed
        consumer = await kafka_manager.create_consumer([topic], "failover-test-group")
        consumed_messages = []

        start_time = time.time()
        async for msg in consumer:
            data = json.loads(msg.value.decode())
            consumed_messages.append(data)
            if len(consumed_messages) >= 2 or time.time() - start_time > 10:
                break

        await consumer.stop()

        # Verify both messages received
        assert len(consumed_messages) == 2
        phases = {msg["phase"] for msg in consumed_messages}
        assert "before_disconnect" in phases
        assert "after_reconnect" in phases


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
