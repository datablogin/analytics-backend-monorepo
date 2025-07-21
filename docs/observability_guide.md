# Comprehensive Observability Stack Guide

## Overview

The Analytics Backend implements a comprehensive observability stack providing complete visibility into system performance, business metrics, and operational health. The stack includes distributed tracing, metrics collection, structured logging, and real-time monitoring with automated alerting.

## Architecture Components

### Core Technologies

- **OpenTelemetry**: Unified observability framework for traces, metrics, and logs
- **Prometheus**: Time-series metrics collection and storage  
- **Jaeger**: Distributed tracing system for request flow analysis
- **Grafana**: Visualization dashboards and alerting
- **AlertManager**: Alert routing and notification management
- **Structured Logging**: Contextual logging with correlation IDs

### System Diagram

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Application    │───▶│ OpenTelemetry│───▶│   Jaeger        │
│  Services       │    │  Collector   │    │  (Tracing)      │
└─────────────────┘    └──────────────┘    └─────────────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Prometheus    │◀───┤   Metrics    │───▶│   Grafana       │
│   (Storage)     │    │  Endpoint    │    │ (Visualization) │
└─────────────────┘    └──────────────┘    └─────────────────┘
         │                                          │
         ▼                                          ▼
┌─────────────────┐                      ┌─────────────────┐
│  AlertManager   │◀─────────────────────┤   Alerting      │
│  (Notifications)│                      │    Rules        │
└─────────────────┘                      └─────────────────┘
```

## Quick Start

### 1. Start Observability Stack

```bash
# Start all observability services
docker-compose -f docker/docker-compose.observability.yml up -d

# Verify services are running
docker-compose -f docker/docker-compose.observability.yml ps
```

### 2. Access Dashboards

- **Grafana**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090
- **Jaeger**: http://localhost:16686
- **AlertManager**: http://localhost:9093

### 3. Enable Observability in Services

```python
from libs.observability import configure_observability, ObservabilityMiddleware

# In your FastAPI application
app = FastAPI(title="Your Service")

# Configure observability
obs_manager = configure_observability(app=app)

# Add observability middleware
app.add_middleware(ObservabilityMiddleware, service_name="your-service")
```

## Configuration

### Environment Variables

```bash
# Observability Configuration
OBSERVABILITY_ENABLED=true
OBSERVABILITY_SERVICE_NAME=analytics-api
OBSERVABILITY_SERVICE_VERSION=1.0.0
OBSERVABILITY_ENVIRONMENT=production

# Tracing Configuration  
OBSERVABILITY_TRACING__ENABLED=true
OBSERVABILITY_TRACING__JAEGER_ENDPOINT=http://localhost:14268/api/traces
OBSERVABILITY_TRACING__SAMPLE_RATE=1.0

# Metrics Configuration
OBSERVABILITY_METRICS__ENABLED=true
OBSERVABILITY_METRICS__PROMETHEUS_PORT=8090
OBSERVABILITY_METRICS__COLLECTION_INTERVAL_SECONDS=15

# Logging Configuration
OBSERVABILITY_LOGGING__LEVEL=INFO
OBSERVABILITY_LOGGING__FORMAT=json
OBSERVABILITY_LOGGING__ENABLE_CORRELATION=true
```

### Programmatic Configuration

```python
from libs.observability import ObservabilityConfig, TracingConfig, MetricsConfig

config = ObservabilityConfig(
    service_name="my-service",
    environment="production",
    tracing=TracingConfig(
        enabled=True,
        sample_rate=0.1,  # Sample 10% of traces in production
        jaeger_endpoint="http://jaeger:14268/api/traces"
    ),
    metrics=MetricsConfig(
        enabled=True,
        prometheus_port=8090,
        enable_runtime_metrics=True
    )
)

obs_manager = configure_observability(config, app)
```

## Metrics Collection

### System Metrics

Automatically collected system-level metrics:

- **HTTP Metrics**: Request count, duration, status codes
- **Database Metrics**: Connection pool, query duration, operations
- **Application Metrics**: Error rates, uptime, memory usage
- **Cache Metrics**: Hit/miss ratios, operation counts

### Business Metrics

Domain-specific metrics for business intelligence:

- **User Activity**: Login rates, active users, session duration
- **Data Processing**: Records processed, validation failures, quality scores
- **API Usage**: Endpoint usage, rate limiting, user types
- **Revenue Metrics**: Report generation, export operations

### Custom Metrics Example

```python
from libs.observability import get_observability_manager

obs_manager = get_observability_manager()
if obs_manager and obs_manager.metrics_collector:
    # Record custom business event
    obs_manager.metrics_collector.record_data_processing(
        dataset="user_events",
        records_count=1000,
        status="success"
    )
    
    # Update data quality score
    obs_manager.metrics_collector.update_data_quality_score(
        dataset="user_events",
        score=94.5
    )
```

## Distributed Tracing

### Automatic Instrumentation

The system automatically instruments:
- FastAPI applications (HTTP requests/responses)
- Database operations (SQLAlchemy)
- Redis operations
- HTTP client requests
- Celery tasks

### Manual Instrumentation

```python
from libs.observability import trace_function, TracingContext

# Function decorator
@trace_function(name="process_user_data", attributes={"component": "user_service"})
async def process_user_data(user_id: str):
    # Your business logic here
    return result

# Context manager
with TracingContext("database_migration", attributes={"version": "1.2.3"}):
    # Migration code here
    pass

# Manual span management
from libs.observability.tracing import get_tracer

tracer = get_tracer(__name__)
with tracer.start_as_current_span("custom_operation") as span:
    span.set_attribute("user_id", user_id)
    span.set_attribute("operation_type", "data_export")
    # Your code here
```

### Span Attributes and Tags

Standard attributes automatically added:
- `service.name`: Service identifier
- `service.version`: Service version
- `http.method`: HTTP method
- `http.url`: Request URL
- `http.status_code`: Response status
- `db.operation`: Database operation type
- `correlation_id`: Request correlation ID

## Structured Logging

### Configuration

```python
from libs.observability.logging import configure_structured_logging, get_logger_with_correlation

# Configure logging
config = LoggingConfig(
    level="INFO",
    format="json",
    enable_correlation=True,
    enable_tracing_integration=True
)
configure_structured_logging(config)

# Get logger with context
logger = get_logger_with_correlation(__name__, component="user_service")
```

### Usage Examples

```python
import structlog
from libs.observability.logging import set_correlation_id

logger = structlog.get_logger(__name__)

# Set correlation ID for request context
set_correlation_id("req-12345")

# Structured logging with context
logger.info(
    "User login successful",
    user_id=user.id,
    login_method="oauth2",
    ip_address=request.client.host,
    duration_ms=123
)

# Error logging with exception
try:
    process_payment(payment_data)
except PaymentError as e:
    logger.error(
        "Payment processing failed",
        user_id=payment_data.user_id,
        amount=payment_data.amount,
        error_code=e.code,
        exc_info=True
    )
```

### Log Correlation

Logs are automatically correlated with:
- **Trace IDs**: Link logs to distributed traces
- **Correlation IDs**: Track requests across services  
- **Span Context**: Associate logs with specific operations
- **Business Context**: User IDs, transaction IDs, etc.

## Monitoring and Alerting

### Key Performance Indicators (KPIs)

#### System Health KPIs
- **Service Availability**: 99.9% uptime SLA
- **Response Time**: 95th percentile < 100ms
- **Error Rate**: < 0.1% for 4xx/5xx responses
- **Database Performance**: Query time < 50ms average

#### Business KPIs  
- **User Engagement**: Active users, session duration
- **Data Quality**: Validation success rate > 95%
- **Processing Throughput**: Records processed per minute
- **API Usage**: Request volume and distribution

### Alert Rules

#### Critical Alerts (Immediate Response)
- Service down (availability < 99%)
- High error rate (> 5% for 2+ minutes)
- Database connection failures
- Memory usage > 90%
- Disk space < 10%

#### High Priority Alerts (15-minute response)
- Increased response time (95th percentile > 200ms)
- Data quality score < 80%
- Authentication failures spike
- Cache hit rate < 70%

#### Warning Alerts (1-hour response)
- CPU usage > 70% for 10+ minutes
- Unusual traffic patterns
- Background job delays
- Low data freshness

### Alert Configuration

```yaml
# Example alert rule (Prometheus format)
groups:
  - name: analytics-backend-alerts
    rules:
      - alert: HighErrorRate
        expr: rate(system_http_requests_total{status_code=~"5.."}[5m]) / rate(system_http_requests_total[5m]) > 0.05
        for: 2m
        labels:
          severity: critical
          service: "{{ $labels.job }}"
        annotations:
          summary: "High error rate detected"
          description: "{{ $labels.job }} has error rate of {{ $value }}%"

      - alert: LowDataQuality
        expr: business_data_quality_score < 80
        for: 5m
        labels:
          severity: high
          category: business
        annotations:
          summary: "Data quality score below threshold"
          description: "Dataset {{ $labels.dataset }} quality score: {{ $value }}%"
```

## Performance Optimization

### Trace Sampling

```python
# Production sampling configuration
tracing_config = TracingConfig(
    enabled=True,
    sample_rate=0.1,  # Sample 10% of traces
    max_export_batch_size=512,
    export_timeout_millis=30000
)
```

### Metrics Optimization

```python
# Efficient metrics collection
metrics_config = MetricsConfig(
    collection_interval_seconds=15,
    enable_runtime_metrics=False,  # Disable in high-volume scenarios
    histogram_buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)
```

### Memory Management

- Use batch processors for trace export
- Configure memory limiters in OTel collector
- Implement metric retention policies
- Regular garbage collection tuning

## Troubleshooting

### Common Issues

#### High Memory Usage
```bash
# Check OTel collector memory
docker stats analytics-otel-collector

# Adjust memory limits in configuration
memory_limiter:
  limit_mib: 512
  spike_limit_mib: 128
```

#### Missing Traces
- Verify Jaeger endpoint connectivity
- Check trace sampling rates
- Validate service instrumentation
- Review OTel collector logs

#### Metrics Not Appearing
- Confirm Prometheus scraping targets
- Check metrics endpoint accessibility
- Verify metric naming conventions
- Review service discovery configuration

### Debugging Tools

```python
# Enable debug logging
import logging
logging.getLogger("opentelemetry").setLevel(logging.DEBUG)

# Check observability status
from libs.observability import get_observability_manager
obs_manager = get_observability_manager()
print(obs_manager.get_metrics_summary())

# Validate configuration
from libs.observability import get_observability_config
config = get_observability_config()
print(f"Tracing enabled: {config.tracing.enabled}")
print(f"Metrics enabled: {config.metrics.enabled}")
```

## Best Practices

### Instrumentation Guidelines

1. **Trace High-Value Operations**: Focus on user-facing APIs and critical business processes
2. **Use Semantic Attributes**: Follow OpenTelemetry semantic conventions
3. **Avoid Sensitive Data**: Never include PII in traces or metrics
4. **Consistent Naming**: Use standardized naming conventions
5. **Error Handling**: Always record exceptions in spans

### Metrics Strategy

1. **Four Golden Signals**: Monitor latency, traffic, errors, and saturation
2. **Business Alignment**: Track metrics that align with business objectives  
3. **Cardinality Management**: Avoid high-cardinality labels
4. **Retention Policies**: Configure appropriate data retention
5. **Aggregation Levels**: Use different aggregation intervals for different use cases

### Alert Management

1. **Severity Classification**: Clear severity levels with defined response times
2. **Alert Fatigue Prevention**: Use appropriate thresholds and suppression rules
3. **Runbook Documentation**: Provide clear remediation steps
4. **Escalation Paths**: Define clear escalation procedures
5. **Regular Reviews**: Periodically review and tune alert rules

## Security Considerations

### Data Privacy
- No PII in traces, metrics, or logs
- Sensitive data masking in log outputs
- Secure transmission (HTTPS/TLS)
- Access control for observability data

### Network Security
- Internal networking for observability stack
- Authentication for dashboard access
- VPN access for production systems
- Regular security audits

## Development and Testing

### Local Development

```bash
# Start observability stack for development
make observability-dev

# Run service with observability
OBSERVABILITY_ENABLED=true make run-api

# Generate test traffic
curl -X GET "http://localhost:8000/health"
curl -X GET "http://localhost:8000/observability/status"
```

### Testing Observability

```python
# Test metrics collection
def test_metrics_collection():
    collector = create_metrics_collector(MetricsConfig())
    collector.record_http_request("GET", "/test", 200, 0.1)
    
    assert collector.system.http_requests_total._value.get() > 0

# Test tracing
@trace_function(name="test_operation")
def test_traced_function():
    return "success"

def test_tracing():
    with patch('libs.observability.tracing.get_tracer') as mock_tracer:
        result = test_traced_function()
        assert result == "success"
        mock_tracer.assert_called()
```

## Maintenance and Operations

### Regular Tasks

- **Weekly**: Review alert effectiveness and adjust thresholds
- **Monthly**: Analyze performance trends and capacity planning
- **Quarterly**: Update observability stack components
- **Annually**: Comprehensive observability stack review

### Capacity Planning

Monitor and plan for:
- Metrics storage growth (Prometheus)
- Trace volume and retention (Jaeger)
- Dashboard performance (Grafana)
- Alert volume and effectiveness

### Backup and Recovery

- Prometheus data backup procedures
- Grafana dashboard exports
- Alert configuration versioning
- Service discovery configuration backup

## Support and Resources

### Internal Documentation
- Service-specific observability guides
- Runbooks for common scenarios
- Alert response procedures
- Escalation contact information

### External Resources
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/)
- [Grafana Tutorial](https://grafana.com/docs/grafana/latest/getting-started/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)