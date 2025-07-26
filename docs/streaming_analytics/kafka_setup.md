# Kafka Configuration Guide

This guide covers setting up and configuring Apache Kafka for the streaming analytics infrastructure.

## Overview

The streaming analytics system uses Kafka for:
- Event streaming and message queuing
- Stream processing with windowed operations
- Dead letter queue handling for failed messages
- Multi-partition event distribution
- Producer/consumer pattern implementation

## Prerequisites

- Apache Kafka 2.8+ (or Confluent Platform)
- Java 11+ (for Kafka broker)
- Python 3.11+ with aiokafka and confluent-kafka packages

## Installation

### Using Docker (Recommended for Development)

1. **Docker Compose Setup**

Create `docker-compose.kafka.yml`:

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: kafka:29092
      KAFKA_CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 1
      KAFKA_CONFLUENT_METRICS_ENABLE: 'true'
      KAFKA_CONFLUENT_SUPPORT_CUSTOMER_ID: anonymous
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
      KAFKA_LOG_SEGMENT_BYTES: 1073741824  # 1GB
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    hostname: kafka-ui
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
```

2. **Start Kafka**

```bash
docker-compose -f docker-compose.kafka.yml up -d
```

3. **Verify Installation**

```bash
# Check if Kafka is running
docker-compose -f docker-compose.kafka.yml ps

# Access Kafka UI at http://localhost:8080
```

### Native Installation

1. **Download and Install Kafka**

```bash
# Download Kafka
wget https://downloads.apache.org/kafka/2.13-3.5.1/kafka_2.13-3.5.1.tgz
tar -xzf kafka_2.13-3.5.1.tgz
cd kafka_2.13-3.5.1

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server
bin/kafka-server-start.sh config/server.properties
```

## Configuration

### Application Configuration

The Kafka configuration is defined in `libs/streaming_analytics/config.py`:

```python
class KafkaConfig(BaseModel):
    # Connection settings
    bootstrap_servers: list[str] = ["localhost:9092"]
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    
    # SSL settings
    ssl_ca_location: str | None = None
    ssl_cert_location: str | None = None
    ssl_key_location: str | None = None
    
    # Producer configuration
    producer_config: dict[str, Any] = {
        "acks": "all",                    # Wait for all in-sync replicas
        "retries": 3,                     # Retry failed sends
        "batch_size": 16384,              # Batch size in bytes
        "linger_ms": 5,                   # Wait time for batching
        "buffer_memory": 33554432,        # Total memory for buffering
        "compression_type": "gzip",       # Compression algorithm
        "enable_idempotence": True,       # Exactly-once semantics
    }
    
    # Consumer configuration
    consumer_config: dict[str, Any] = {
        "auto_offset_reset": "earliest",  # Start from beginning if no offset
        "enable_auto_commit": False,      # Manual offset commits
        "max_poll_records": 500,          # Max records per poll
        "session_timeout_ms": 30000,      # Session timeout
        "heartbeat_interval_ms": 3000,    # Heartbeat interval
        "fetch_min_bytes": 1024,          # Minimum fetch size
        "fetch_max_wait_ms": 500,         # Max wait for fetch
    }
    
    # Dead letter queue
    enable_dead_letter_queue: bool = True
    dead_letter_topic_suffix: str = ".dlq"
```

### Environment Variables

Configure using environment variables with `STREAMING_KAFKA__` prefix:

```bash
# Basic connection
STREAMING_KAFKA__BOOTSTRAP_SERVERS='["broker1:9092","broker2:9092"]'
STREAMING_KAFKA__SECURITY_PROTOCOL=SASL_SSL

# SASL authentication
STREAMING_KAFKA__SASL_MECHANISM=PLAIN
STREAMING_KAFKA__SASL_USERNAME=your-username
STREAMING_KAFKA__SASL_PASSWORD=your-password

# SSL configuration
STREAMING_KAFKA__SSL_CA_LOCATION=/path/to/ca-cert.pem
STREAMING_KAFKA__SSL_CERT_LOCATION=/path/to/client-cert.pem
STREAMING_KAFKA__SSL_KEY_LOCATION=/path/to/client-key.pem

# Producer settings
STREAMING_KAFKA__PRODUCER_CONFIG='{"acks":"all","compression_type":"lz4"}'

# Consumer settings  
STREAMING_KAFKA__CONSUMER_CONFIG='{"auto_offset_reset":"latest"}'
```

### Security Configuration

#### SASL/PLAIN Authentication

```bash
STREAMING_KAFKA__SECURITY_PROTOCOL=SASL_PLAINTEXT
STREAMING_KAFKA__SASL_MECHANISM=PLAIN
STREAMING_KAFKA__SASL_USERNAME=your-username
STREAMING_KAFKA__SASL_PASSWORD=your-password
```

#### SASL/SSL with TLS

```bash
STREAMING_KAFKA__SECURITY_PROTOCOL=SASL_SSL
STREAMING_KAFKA__SASL_MECHANISM=PLAIN
STREAMING_KAFKA__SASL_USERNAME=your-username
STREAMING_KAFKA__SASL_PASSWORD=your-password
STREAMING_KAFKA__SSL_CA_LOCATION=/path/to/ca-cert.pem
```

#### mTLS (Mutual TLS)

```bash
STREAMING_KAFKA__SECURITY_PROTOCOL=SSL
STREAMING_KAFKA__SSL_CA_LOCATION=/path/to/ca-cert.pem
STREAMING_KAFKA__SSL_CERT_LOCATION=/path/to/client-cert.pem  
STREAMING_KAFKA__SSL_KEY_LOCATION=/path/to/client-key.pem
```

## Topic Configuration

### Default Topics

The system uses these default topics (configurable via environment):

```bash
STREAMING_EVENTS_TOPIC=analytics-events
STREAMING_METRICS_TOPIC=streaming-metrics
STREAMING_ALERTS_TOPIC=streaming-alerts
STREAMING_ML_PREDICTIONS_TOPIC=ml-predictions
```

### Topic Creation

Topics are automatically created by the `KafkaTopicManager`:

```python
from libs.streaming_analytics.kafka_manager import KafkaTopicManager
from libs.streaming_analytics.config import get_streaming_config

config = get_streaming_config()
topic_manager = KafkaTopicManager(config.kafka)

# Create topic with custom configuration
await topic_manager.create_topic(
    topic_name="analytics-events",
    num_partitions=6,
    replication_factor=3,
    config={
        "cleanup.policy": "delete",
        "retention.ms": "604800000",  # 7 days
        "compression.type": "gzip",
        "min.insync.replicas": "2"
    }
)
```

### Production Topic Settings

For production environments, use these recommended settings:

```python
production_topic_config = {
    "cleanup.policy": "delete",
    "retention.ms": "2592000000",        # 30 days
    "retention.bytes": "1073741824000",  # 1TB per partition
    "segment.ms": "86400000",            # 1 day segments
    "compression.type": "lz4",           # Fast compression
    "min.insync.replicas": "2",          # Durability
    "unclean.leader.election.enable": "false",
    "max.message.bytes": "1048576",      # 1MB max message
}
```

## Performance Tuning

### Producer Performance

```python
high_throughput_producer_config = {
    "acks": "1",                    # Leader acknowledgment only
    "retries": 3,
    "batch_size": 65536,            # Larger batches (64KB)
    "linger_ms": 10,                # Wait longer for batching
    "buffer_memory": 134217728,     # More memory (128MB)
    "compression_type": "lz4",      # Fast compression
    "enable_idempotence": False,    # Disable for higher throughput
}

low_latency_producer_config = {
    "acks": "1",
    "retries": 0,                   # No retries for lowest latency
    "batch_size": 1,                # No batching
    "linger_ms": 0,                 # Send immediately
    "compression_type": "none",     # No compression overhead
}
```

### Consumer Performance

```python
high_throughput_consumer_config = {
    "fetch_min_bytes": 65536,       # Wait for more data (64KB)
    "fetch_max_wait_ms": 500,       # Wait up to 500ms
    "max_poll_records": 5000,       # Process more records per poll
    "receive_buffer_bytes": 262144,  # Larger socket buffer (256KB)
}

low_latency_consumer_config = {
    "fetch_min_bytes": 1,           # Don't wait for data
    "fetch_max_wait_ms": 0,         # Return immediately
    "max_poll_records": 100,        # Process fewer records
}
```

### Partitioning Strategy

```python
# Number of partitions should be:
# - Multiple of number of consumers in group
# - Consider throughput requirements
# - Account for future scaling needs

num_partitions = max(
    num_consumers_in_group,
    target_throughput_mb_per_sec / 10,  # ~10MB/s per partition
    6  # Minimum for good parallelism
)
```

## Monitoring and Metrics

### Built-in Metrics

The `KafkaManager` provides built-in metrics:

```python
from libs.streaming_analytics.kafka_manager import KafkaManager

manager = KafkaManager()
stats = await manager.get_producer_stats()

print(f"Messages sent: {stats['messages_sent']}")
print(f"Bytes sent: {stats['bytes_sent']}")
print(f"Send errors: {stats['send_errors']}")
```

### JMX Metrics

Enable JMX for detailed Kafka metrics:

```bash
# Kafka server JMX settings
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote.port=9999"
```

### Prometheus Integration

Use the Kafka Exporter for Prometheus metrics:

```yaml
version: '3.8'
services:
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    ports:
      - "9308:9308"
    command:
      - --kafka.server=kafka:29092
      - --web.listen-address=:9308
    depends_on:
      - kafka
```

## Development Workflow

### Local Development Setup

1. **Start Kafka with Docker**

```bash
# Use the provided docker-compose file
docker-compose -f docker-compose.kafka.yml up -d

# Verify Kafka is running
docker logs kafka
```

2. **Create Development Topics**

```python
# scripts/setup_kafka_topics.py
import asyncio
from libs.streaming_analytics.kafka_manager import KafkaTopicManager
from libs.streaming_analytics.config import get_streaming_config

async def setup_topics():
    config = get_streaming_config()
    manager = KafkaTopicManager(config.kafka)
    
    topics = [
        "analytics-events",
        "streaming-metrics", 
        "streaming-alerts",
        "ml-predictions"
    ]
    
    for topic in topics:
        await manager.create_topic(
            topic_name=topic,
            num_partitions=3,
            replication_factor=1
        )
        print(f"Created topic: {topic}")

if __name__ == "__main__":
    asyncio.run(setup_topics())
```

3. **Run Setup Script**

```bash
python scripts/setup_kafka_topics.py
```

### Testing Kafka Integration

```python
# Test producer/consumer functionality
import asyncio
from libs.streaming_analytics.kafka_manager import KafkaManager

async def test_kafka():
    manager = KafkaManager()
    
    # Test producer
    test_event = {
        "event_id": "test_123",
        "event_type": "test_event",
        "timestamp": "2024-01-01T00:00:00Z",
        "data": {"message": "Hello Kafka!"}
    }
    
    await manager.produce_event("analytics-events", test_event)
    print("Event produced successfully")
    
    # Test consumer
    async def handle_message(message):
        print(f"Received message: {message}")
    
    await manager.consume_events("analytics-events", handle_message)

asyncio.run(test_kafka())
```

## Production Deployment

### Kafka Cluster Setup

1. **Multi-Broker Configuration**

```properties
# server-1.properties
broker.id=1
listeners=PLAINTEXT://kafka1:9092
advertised.listeners=PLAINTEXT://kafka1:9092
log.dirs=/kafka-logs-1

# server-2.properties  
broker.id=2
listeners=PLAINTEXT://kafka2:9092
advertised.listeners=PLAINTEXT://kafka2:9092
log.dirs=/kafka-logs-2

# server-3.properties
broker.id=3
listeners=PLAINTEXT://kafka3:9092
advertised.listeners=PLAINTEXT://kafka3:9092
log.dirs=/kafka-logs-3
```

2. **High Availability Settings**

```properties
# Replication and durability
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false

# Performance
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Log settings
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
```

### Application Configuration

```bash
# Production environment variables
STREAMING_KAFKA__BOOTSTRAP_SERVERS='["kafka1:9092","kafka2:9092","kafka3:9092"]'
STREAMING_KAFKA__SECURITY_PROTOCOL=SASL_SSL
STREAMING_KAFKA__SASL_MECHANISM=PLAIN
STREAMING_KAFKA__PRODUCER_CONFIG='{"acks":"all","retries":5,"enable.idempotence":true}'
STREAMING_KAFKA__CONSUMER_CONFIG='{"session.timeout.ms":45000,"heartbeat.interval.ms":3000}'
```

## Troubleshooting

### Common Issues

#### Connection Errors

```bash
# Check if Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker logs kafka

# Test connection
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

#### Authentication Issues

```bash
# Verify SASL configuration
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic test-topic \
  --producer.config client.properties
```

#### Performance Issues

```bash
# Monitor consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group your-consumer-group --describe

# Monitor topic metrics
kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic analytics-events --describe
```

### Error Handling

The system provides comprehensive error handling:

```python
try:
    await kafka_manager.produce_event(topic, event)
except KafkaError as e:
    logger.error("Kafka error", error=str(e))
    # Event will be sent to dead letter queue if configured
except TimeoutError:
    logger.warning("Kafka timeout", topic=topic)
    # Retry logic will handle this
except Exception as e:
    logger.error("Unexpected error", error=str(e))
    # Log and continue processing
```

### Dead Letter Queue

Failed messages are automatically routed to dead letter topics:

```python
# Original topic: analytics-events
# Dead letter topic: analytics-events.dlq

# Monitor dead letter queue
await kafka_manager.consume_events(
    "analytics-events.dlq", 
    handle_failed_message
)
```

## Best Practices

### Topic Design
- Use kebab-case for topic names
- Include environment prefix for multi-environment setups
- Keep topic names descriptive and consistent
- Plan partition strategy based on consumer parallelism

### Message Design
- Use consistent message schemas
- Include correlation IDs for tracing
- Keep messages lightweight (< 1MB)
- Use proper serialization (JSON, Avro, Protocol Buffers)

### Consumer Groups
- Use descriptive consumer group names
- One consumer group per application instance
- Handle rebalancing gracefully
- Commit offsets after processing

### Error Handling
- Implement exponential backoff for retries
- Use dead letter queues for poison messages
- Log all errors with context
- Monitor consumer lag and error rates

### Security
- Use TLS for all connections
- Implement proper authentication
- Rotate credentials regularly
- Network isolation for Kafka clusters