# Streaming Analytics Documentation

Comprehensive documentation for the streaming analytics infrastructure, covering setup, configuration, deployment, and troubleshooting.

## Overview

The streaming analytics system provides real-time event processing, machine learning inference, and live dashboard updates with:

- **High Throughput**: 100,000+ events/second processing capacity
- **Low Latency**: <100ms end-to-end processing latency  
- **Real-time ML**: <50ms machine learning inference
- **WebSocket API**: Live dashboard updates with <10ms delivery
- **High Availability**: 99.9% uptime with auto-scaling and failover

## Architecture

```
Event Sources → Kafka → Stream Processing → ML Inference → WebSocket API
                ↓            ↓              ↓            ↓
            Event Store   Aggregations  Predictions   Live Updates
```

## Documentation Structure

### Quick Start Guides

1. **[WebSocket API](./websocket_api.md)** - Real-time WebSocket API documentation
   - Connection management and authentication
   - Message types and subscription patterns
   - Client examples in JavaScript and Python
   - Security features and rate limiting

2. **[Kafka Setup](./kafka_setup.md)** - Apache Kafka configuration and setup
   - Installation with Docker and native deployment
   - Producer/consumer optimization
   - Topic configuration and management
   - Security and authentication setup

### Integration Guides

3. **[ML Model Integration](./ml_integration.md)** - Machine learning model integration
   - Model registry integration with MLflow
   - Real-time feature extraction and validation
   - Batch and streaming inference patterns
   - A/B testing and ensemble methods

4. **[Monitoring & Alerting](./monitoring_alerting.md)** - Comprehensive monitoring setup
   - Metrics collection and visualization
   - Alert rules and notification channels
   - SLA monitoring and error budget tracking
   - Auto-scaling based on performance metrics

### Operations Guides

5. **[Production Deployment](./deployment.md)** - Production deployment guide
   - Kubernetes orchestration and scaling
   - Security configuration and network policies
   - CI/CD pipeline setup with GitHub Actions
   - Infrastructure as Code with Helm charts

6. **[Performance Tuning](./performance_tuning.md)** - Performance optimization guide
   - Kafka producer/consumer tuning
   - Stream processing optimization
   - ML inference performance improvements
   - Memory management and garbage collection

7. **[Troubleshooting](./troubleshooting.md)** - Comprehensive troubleshooting guide
   - Common issues and diagnostic procedures
   - Performance debugging techniques
   - Emergency recovery procedures
   - Monitoring and health check scripts

## Quick Start

### Prerequisites

- Python 3.11+
- Apache Kafka 2.8+
- PostgreSQL 13+
- Redis 6+
- Docker and Kubernetes (for production)

### Development Setup

1. **Install Dependencies**
   ```bash
   # From the monorepo root
   make dev-install
   ```

2. **Start Local Kafka**
   ```bash
   # Using Docker Compose
   cd docker
   docker-compose -f docker-compose.kafka.yml up -d
   ```

3. **Initialize Database**
   ```bash
   # Run migrations
   make migrate-upgrade
   ```

4. **Start Services**
   ```bash
   # WebSocket server
   python -m libs.streaming_analytics.websocket_server
   
   # Stream processor
   python -m libs.streaming_analytics.processor
   
   # ML inference service
   python -m libs.streaming_analytics.realtime_ml
   ```

### Basic Usage Example

```python
from libs.streaming_analytics import (
    StreamingConfig,
    KafkaManager, 
    StreamProcessor,
    RealtimeMLPipeline,
    WebSocketServer
)

# Initialize components
config = StreamingConfig()
kafka_manager = KafkaManager(config.kafka)
ml_pipeline = RealtimeMLPipeline(config.realtime_ml)
websocket_server = WebSocketServer(config.websocket)

# Start services
await kafka_manager.initialize()
await ml_pipeline.initialize()
await websocket_server.start()

# Process events
async def process_event(event):
    # Make ML prediction
    prediction = await ml_pipeline.predict_single(event)
    
    # Broadcast to WebSocket clients
    await websocket_server.broadcast_prediction(prediction)

# Start stream processing
processor = StreamProcessor(process_event)
await processor.consume_events("analytics-events")
```

## Key Components

### Event Processing Pipeline

- **Event Store** (`libs/streaming_analytics/event_store.py`): Event schema validation and storage
- **Kafka Manager** (`libs/streaming_analytics/kafka_manager.py`): Kafka producer/consumer management
- **Stream Processor** (`libs/streaming_analytics/processor.py`): Windowed stream processing
- **Circuit Breaker** (`libs/streaming_analytics/circuit_breaker.py`): Fault tolerance patterns

### Machine Learning Infrastructure

- **Realtime ML Pipeline** (`libs/streaming_analytics/realtime_ml.py`): Real-time ML inference
- **Model Registry Integration**: MLflow model loading and caching
- **Feature Store Integration**: Real-time feature retrieval
- **Batch Processing**: Efficient batch inference for high throughput

### WebSocket Infrastructure

- **WebSocket Server** (`libs/streaming_analytics/websocket_server.py`): Real-time client connections
- **Subscription Management**: Topic-based message filtering
- **Authentication & Security**: Token-based auth with rate limiting
- **Connection Pooling**: Optimized connection management

### Monitoring & Observability

- **Streaming Metrics** (`libs/streaming_analytics/monitoring.py`): Performance metrics collection
- **Distributed Tracing**: OpenTelemetry integration
- **Health Checks**: Component health monitoring
- **Auto-scaling**: Dynamic scaling based on load

## Performance Targets

| Metric | Target | Description |
|--------|--------|-------------|
| **Event Throughput** | 100,000 events/sec | Peak event processing capacity |
| **Processing Latency** | <100ms P95 | End-to-end event processing time |
| **ML Inference** | <50ms P95 | Machine learning prediction latency |
| **WebSocket Delivery** | <10ms P95 | Real-time message delivery time |
| **Availability** | 99.9% | System uptime target |
| **Error Rate** | <0.1% | Maximum acceptable error rate |

## Configuration

### Environment Variables

Core configuration uses the `STREAMING_` prefix:

```bash
# Kafka configuration
STREAMING_KAFKA__BOOTSTRAP_SERVERS='["kafka1:9092","kafka2:9092"]'
STREAMING_KAFKA__SECURITY_PROTOCOL=SASL_SSL

# WebSocket configuration  
STREAMING_WEBSOCKET__HOST=0.0.0.0
STREAMING_WEBSOCKET__PORT=8765
STREAMING_WEBSOCKET__MAX_CONNECTIONS=10000

# ML configuration
STREAMING_REALTIME_ML__MODEL_CACHE_SIZE=20
STREAMING_REALTIME_ML__MAX_LATENCY_MS=50

# Monitoring configuration
STREAMING_MONITORING__METRICS_INTERVAL_SECONDS=10
STREAMING_MONITORING__ENABLE_AUTO_SCALING=true
```

### Topics Configuration

Default Kafka topics (configurable):

- `analytics-events`: Main event stream
- `streaming-metrics`: System metrics
- `streaming-alerts`: Alert notifications  
- `ml-predictions`: ML prediction results

## Security

### Authentication
- JWT-based authentication for WebSocket connections
- SASL/SSL for Kafka connections
- TLS encryption for all external communication

### Authorization
- Role-based access control (RBAC)
- Topic-level permissions
- Rate limiting per client

### Network Security
- Network policies for Kubernetes deployment
- Private subnets and VPC isolation
- TLS termination at load balancer

## Monitoring

### Key Metrics to Monitor

- **Event Processing Rate**: `rate(events_processed_total[5m])`
- **Error Rate**: `rate(events_failed_total[5m]) / rate(events_processed_total[5m])`
- **Processing Latency**: `histogram_quantile(0.95, rate(event_processing_duration_ms_bucket[5m]))`
- **Consumer Lag**: `kafka_consumer_lag`
- **WebSocket Connections**: `websocket_connections_active`
- **ML Inference Latency**: `histogram_quantile(0.95, rate(ml_inference_duration_ms_bucket[5m]))`

### Alerting Rules

Critical alerts to set up:

- High error rate (>5% for 2 minutes)
- High processing latency (>1s P95 for 5 minutes)
- Consumer lag (>10k messages for 5 minutes)
- ML inference failures (>10% error rate for 2 minutes)
- WebSocket connection drops (>10/sec for 2 minutes)

## Testing

### Unit Tests
```bash
# Run all streaming analytics tests
pytest tests/libs/test_streaming_analytics.py -v

# Run specific component tests
pytest tests/test_streaming_analytics.py::test_kafka_integration -v
pytest tests/test_streaming_analytics.py::test_websocket_server -v
pytest tests/test_streaming_analytics.py::test_ml_pipeline -v
```

### Integration Tests
```bash
# Run end-to-end integration tests
pytest tests/integration/test_streaming_e2e.py -v

# Run with coverage
pytest tests/ --cov=libs.streaming_analytics --cov-report=html
```

### Load Testing
```bash
# Run load tests
python tests/integration/load_test_streaming.py

# WebSocket load testing
python tests/integration/websocket_load_test.py
```

## Support and Contributing

### Getting Help

1. Check the [Troubleshooting Guide](./troubleshooting.md) for common issues
2. Review [Performance Tuning](./performance_tuning.md) for optimization tips
3. Consult [Monitoring & Alerting](./monitoring_alerting.md) for observability setup
4. Create an issue in the GitHub repository for bugs or feature requests

### Contributing

1. Follow the existing code patterns and architecture
2. Add comprehensive tests for new features
3. Update documentation for any API changes
4. Run the full test suite before submitting PRs
5. Include performance impact analysis for significant changes

### Architecture Decisions

Key architectural decisions and trade-offs:

- **Async/Await**: All I/O operations use async patterns for better concurrency
- **Type Safety**: Strict typing with Pydantic for data validation
- **Observability First**: Comprehensive metrics and tracing built-in
- **Fault Tolerance**: Circuit breakers and retry logic throughout
- **Horizontal Scaling**: Designed for Kubernetes deployment with auto-scaling

## License

This streaming analytics infrastructure is part of the Analytics Backend Monorepo. See the main repository LICENSE file for details.

## Changelog

### v1.0.0 (Current)
- Initial streaming analytics infrastructure
- WebSocket API with authentication and rate limiting
- Kafka integration with producer/consumer optimization
- Real-time ML inference pipeline
- Comprehensive monitoring and alerting
- Production-ready Kubernetes deployment
- Complete documentation suite

For detailed version history, see the main repository CHANGELOG.md.