# Monitoring and Alerting Setup Guide

This guide covers comprehensive monitoring and alerting for the streaming analytics infrastructure.

## Overview

The streaming analytics monitoring system provides:

- **Real-time Metrics**: Performance, throughput, and error rate monitoring
- **Distributed Tracing**: End-to-end request tracing across components
- **Auto-scaling**: Dynamic scaling based on load metrics
- **Alerting**: Multi-channel alerts for critical issues
- **Dashboard Integration**: Pre-built Grafana dashboards
- **SLA Monitoring**: Service level agreement tracking

## Architecture

```
Streaming Components → Metrics Collection → Storage → Visualization/Alerting
       ↓                      ↓               ↓            ↓
   OpenTelemetry         Prometheus       Grafana     PagerDuty/Slack
```

## Core Components

### StreamingMetrics

The main metrics collection class in `libs/streaming_analytics/monitoring.py`:

```python
from libs.streaming_analytics.monitoring import StreamingMetrics

metrics = StreamingMetrics()

# Counter metrics
metrics.increment_counter(
    "events_processed_total",
    tags={"topic": "analytics-events", "partition": "0"}
)

# Histogram metrics  
metrics.record_histogram(
    "event_processing_duration_ms",
    processing_time_ms,
    tags={"processor": "ml_inference"}
)

# Gauge metrics
metrics.set_gauge(
    "active_connections",
    connection_count,
    tags={"service": "websocket_server"}
)
```

### Configuration

Monitoring configuration in `libs/streaming_analytics/config.py`:

```python
class MonitoringConfig(BaseModel):
    # Metrics collection
    metrics_interval_seconds: int = 10      # Collection interval
    detailed_metrics: bool = True           # Enable detailed metrics

    # Performance thresholds
    latency_warning_ms: int = 100          # Warning threshold
    latency_critical_ms: int = 1000        # Critical threshold
    throughput_warning_threshold: float = 0.8    # 80% of target
    error_rate_warning_threshold: float = 0.01   # 1%
    error_rate_critical_threshold: float = 0.05  # 5%

    # Auto-scaling
    enable_auto_scaling: bool = True
    scale_up_threshold: float = 0.8        # Scale up at 80% load
    scale_down_threshold: float = 0.3      # Scale down at 30% load
    min_instances: int = 1
    max_instances: int = 10
    cooldown_period_seconds: int = 300     # 5-minute cooldown
```

### Environment Variables

```bash
# Metrics settings
STREAMING_MONITORING__METRICS_INTERVAL_SECONDS=5
STREAMING_MONITORING__DETAILED_METRICS=true

# Performance thresholds
STREAMING_MONITORING__LATENCY_WARNING_MS=50
STREAMING_MONITORING__LATENCY_CRITICAL_MS=500
STREAMING_MONITORING__ERROR_RATE_WARNING_THRESHOLD=0.005

# Auto-scaling
STREAMING_MONITORING__ENABLE_AUTO_SCALING=true
STREAMING_MONITORING__MIN_INSTANCES=2
STREAMING_MONITORING__MAX_INSTANCES=20
```

## Metrics Collection

### Built-in Metrics

The system automatically collects these metrics:

#### Event Processing Metrics
```python
# Event throughput
"events_processed_total"          # Counter: Total events processed
"events_per_second"              # Gauge: Current event rate
"event_processing_duration_ms"   # Histogram: Processing latency

# Event quality
"events_failed_total"            # Counter: Failed events
"events_retried_total"           # Counter: Retried events
"dead_letter_queue_size"         # Gauge: DLQ backlog
```

#### Stream Processing Metrics
```python
# Window processing
"windows_processed_total"        # Counter: Windows processed
"window_processing_duration_ms"  # Histogram: Window processing time
"window_late_events_total"       # Counter: Late arriving events

# Aggregation performance
"aggregation_operations_total"   # Counter: Aggregation operations
"aggregation_duration_ms"        # Histogram: Aggregation latency
"aggregation_memory_usage_bytes" # Gauge: Memory usage
```

#### ML Inference Metrics
```python
# Prediction performance
"ml_predictions_total"           # Counter: Total predictions
"ml_inference_duration_ms"       # Histogram: Inference latency
"ml_batch_size"                  # Histogram: Batch sizes used

# Model management
"ml_models_loaded"               # Gauge: Models in cache
"ml_model_refresh_total"         # Counter: Model refreshes
"ml_prediction_errors_total"     # Counter: Prediction failures
```

#### WebSocket Metrics
```python
# Connection metrics
"websocket_connections_active"   # Gauge: Active connections
"websocket_connections_total"    # Counter: Total connections
"websocket_messages_sent_total"  # Counter: Messages sent
"websocket_messages_received_total"  # Counter: Messages received

# Performance metrics
"websocket_message_duration_ms"  # Histogram: Message processing time
"websocket_broadcast_duration_ms"    # Histogram: Broadcast latency
```

#### Kafka Metrics
```python
# Producer metrics
"kafka_messages_sent_total"      # Counter: Messages sent
"kafka_send_duration_ms"         # Histogram: Send latency
"kafka_send_errors_total"        # Counter: Send errors

# Consumer metrics
"kafka_messages_consumed_total"  # Counter: Messages consumed
"kafka_consumer_lag"             # Gauge: Consumer lag
"kafka_rebalances_total"         # Counter: Rebalances
```

### Custom Metrics

Add custom metrics for your specific needs:

```python
from libs.streaming_analytics.monitoring import StreamingMetrics

class CustomMetricsCollector:
    def __init__(self):
        self.metrics = StreamingMetrics()
    
    async def track_business_metrics(self, event: EventSchema):
        """Track business-specific metrics."""
        
        # User engagement metrics
        if event.event_type == "user_action":
            self.metrics.increment_counter(
                "user_actions_total",
                tags={
                    "action_type": event.payload.get("action_type"),
                    "user_segment": self.get_user_segment(event)
                }
            )
            
            # Session duration tracking
            session_duration = event.payload.get("session_duration", 0)
            self.metrics.record_histogram(
                "user_session_duration_seconds",
                session_duration,
                tags={"user_segment": self.get_user_segment(event)}
            )
        
        # Revenue metrics
        if event.event_type == "purchase":
            purchase_amount = event.payload.get("amount", 0)
            self.metrics.record_histogram(
                "purchase_amount_usd",
                purchase_amount,
                tags={"currency": event.payload.get("currency", "USD")}
            )
            
            self.metrics.increment_counter(
                "revenue_events_total",
                tags={"product_category": event.payload.get("category")}
            )
    
    def get_user_segment(self, event: EventSchema) -> str:
        """Determine user segment for metrics tagging."""
        # Implementation depends on your business logic
        user_id = event.payload.get("user_id")
        # Query user service or cache for segment info
        return "premium"  # Simplified example
```

## Alerting Configuration

### Alert Rules

Define alert rules using the monitoring framework:

```python
from libs.streaming_analytics.monitoring import AlertRule, AlertSeverity

class StreamingAlertRules:
    def __init__(self):
        self.rules = [
            # High error rate alert
            AlertRule(
                name="high_error_rate",
                condition="rate(events_failed_total[5m]) > 0.05",
                severity=AlertSeverity.CRITICAL,
                description="Event processing error rate > 5%",
                channels=["pagerduty", "slack"],
                cooldown_minutes=10
            ),
            
            # High latency alert
            AlertRule(
                name="high_processing_latency",
                condition="histogram_quantile(0.95, event_processing_duration_ms[5m]) > 1000",
                severity=AlertSeverity.WARNING,
                description="95th percentile processing latency > 1s",
                channels=["slack"],
                cooldown_minutes=15
            ),
            
            # Consumer lag alert
            AlertRule(
                name="kafka_consumer_lag",
                condition="kafka_consumer_lag > 10000",
                severity=AlertSeverity.WARNING,
                description="Kafka consumer lag > 10k messages",
                channels=["slack"],
                cooldown_minutes=5
            ),
            
            # Model prediction failures
            AlertRule(
                name="ml_prediction_failures",
                condition="rate(ml_prediction_errors_total[5m]) > 0.1",
                severity=AlertSeverity.CRITICAL,
                description="ML prediction error rate > 10%",
                channels=["pagerduty", "email"],
                cooldown_minutes=5
            ),
            
            # WebSocket connection issues
            AlertRule(
                name="websocket_connection_drops",
                condition="rate(websocket_connections_total[5m]) - rate(websocket_disconnections_total[5m]) < -10",
                severity=AlertSeverity.WARNING,
                description="High rate of WebSocket disconnections",
                channels=["slack"],
                cooldown_minutes=10
            )
        ]
```

### Notification Channels

Configure multiple notification channels:

```python
from libs.streaming_analytics.monitoring import NotificationChannel

# Slack integration
slack_channel = NotificationChannel(
    name="slack",
    type="slack",
    config={
        "webhook_url": "https://hooks.slack.com/services/...",
        "channel": "#streaming-alerts",
        "username": "StreamingBot",
        "icon_emoji": ":warning:"
    }
)

# PagerDuty integration
pagerduty_channel = NotificationChannel(
    name="pagerduty",
    type="pagerduty",
    config={
        "integration_key": "your-pagerduty-integration-key",
        "severity_mapping": {
            "WARNING": "warning",
            "CRITICAL": "critical"
        }
    }
)

# Email notifications
email_channel = NotificationChannel(
    name="email",
    type="email",
    config={
        "smtp_server": "smtp.company.com",
        "smtp_port": 587,
        "username": "alerts@company.com",
        "password": "smtp-password",
        "recipients": ["team@company.com"],
        "from_address": "streaming-alerts@company.com"
    }
)
```

### Environment Variables for Alerting

```bash
# Slack alerts
STREAMING_ALERTS__SLACK__WEBHOOK_URL=https://hooks.slack.com/services/...
STREAMING_ALERTS__SLACK__CHANNEL=#streaming-alerts

# PagerDuty alerts
STREAMING_ALERTS__PAGERDUTY__INTEGRATION_KEY=your-integration-key

# Email alerts
STREAMING_ALERTS__EMAIL__SMTP_SERVER=smtp.company.com
STREAMING_ALERTS__EMAIL__RECIPIENTS=["team@company.com","oncall@company.com"]
```

## Observability Integration

### OpenTelemetry Setup

The system uses OpenTelemetry for distributed tracing:

```python
from libs.observability import configure_tracing

# Configure tracing for streaming components
configure_tracing(
    service_name="streaming-analytics",
    jaeger_endpoint="http://jaeger:14268/api/traces",
    sampling_rate=0.1  # Sample 10% of traces
)
```

### Trace Context Propagation

Automatic trace context propagation across components:

```python
from libs.streaming_analytics import StreamProcessor
from libs.observability.tracing import trace_async

class TracedStreamProcessor(StreamProcessor):
    @trace_async("stream_processor.process_event")
    async def process_event(self, event: EventSchema) -> None:
        """Process event with distributed tracing."""
        
        # Trace context is automatically propagated
        with tracer.start_as_current_span("feature_extraction"):
            features = await self.extract_features(event)
        
        with tracer.start_as_current_span("ml_prediction"):
            prediction = await self.make_prediction(features)
        
        with tracer.start_as_current_span("result_publishing"):
            await self.publish_result(prediction)
```

### Custom Spans

Add custom spans for detailed tracing:

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

async def process_user_event(event: UserActionEvent):
    """Process user event with detailed tracing."""
    
    with tracer.start_as_current_span("user_event_processing") as span:
        # Add span attributes
        span.set_attribute("user_id", event.user_id)
        span.set_attribute("event_type", event.event_type)
        span.set_attribute("session_id", event.session_id)
        
        # Child spans for sub-operations
        with tracer.start_as_current_span("user_lookup"):
            user_data = await get_user_data(event.user_id)
            span.set_attribute("user_segment", user_data.segment)
        
        with tracer.start_as_current_span("churn_prediction"):
            prediction_result = await predict_churn(event, user_data)
            span.set_attribute("churn_probability", prediction_result.probability)
        
        # Record events
        span.add_event("prediction_completed", {
            "model_version": prediction_result.model_version,
            "confidence": prediction_result.confidence
        })
```

## Dashboard Setup

### Grafana Dashboards

Pre-built Grafana dashboard configurations:

#### 1. Streaming Overview Dashboard

```json
{
  "dashboard": {
    "title": "Streaming Analytics Overview",
    "panels": [
      {
        "title": "Event Processing Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(events_processed_total[5m])",
            "legendFormat": "{{topic}} - {{partition}}"
          }
        ]
      },
      {
        "title": "Processing Latency",
        "type": "graph", 
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(event_processing_duration_ms_bucket[5m]))",
            "legendFormat": "95th percentile"
          },
          {
            "expr": "histogram_quantile(0.50, rate(event_processing_duration_ms_bucket[5m]))",
            "legendFormat": "50th percentile"
          }
        ]
      },
      {
        "title": "Error Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(events_failed_total[5m]) / rate(events_processed_total[5m]) * 100",
            "legendFormat": "Error Rate %"
          }
        ],
        "thresholds": [
          {"color": "green", "value": 0},
          {"color": "yellow", "value": 1},
          {"color": "red", "value": 5}
        ]
      }
    ]
  }
}
```

#### 2. ML Inference Dashboard

```json
{
  "dashboard": {
    "title": "ML Inference Monitoring",
    "panels": [
      {
        "title": "Prediction Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(ml_predictions_total[5m])",
            "legendFormat": "{{model_name}}"
          }
        ]
      },
      {
        "title": "Inference Latency Distribution",
        "type": "heatmap",
        "targets": [
          {
            "expr": "rate(ml_inference_duration_ms_bucket[5m])",
            "legendFormat": "{{le}}"
          }
        ]
      },
      {
        "title": "Model Cache Hit Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(ml_model_cache_hits[5m]) / (rate(ml_model_cache_hits[5m]) + rate(ml_model_cache_misses[5m])) * 100"
          }
        ]
      }
    ]
  }
}
```

#### 3. WebSocket Connections Dashboard

```json
{
  "dashboard": {
    "title": "WebSocket Server Monitoring",
    "panels": [
      {
        "title": "Active Connections",
        "type": "graph",
        "targets": [
          {
            "expr": "websocket_connections_active",
            "legendFormat": "Active Connections"
          }
        ]
      },
      {
        "title": "Message Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(websocket_messages_sent_total[5m])",
            "legendFormat": "Messages Sent/sec"
          },
          {
            "expr": "rate(websocket_messages_received_total[5m])",
            "legendFormat": "Messages Received/sec"
          }
        ]
      },
      {
        "title": "Connection Distribution by Origin",
        "type": "pie",
        "targets": [
          {
            "expr": "websocket_connections_active",
            "legendFormat": "{{origin}}"
          }
        ]
      }
    ]
  }
}
```

### Dashboard Deployment

Deploy dashboards using Grafana provisioning:

```yaml
# docker/observability/grafana/provisioning/dashboards/streaming-analytics.yml
apiVersion: 1

providers:
  - name: 'streaming-analytics'
    orgId: 1
    folder: 'Streaming Analytics'
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /etc/grafana/provisioning/dashboards/streaming
```

## Auto-scaling Setup

### Kubernetes HPA Integration

Horizontal Pod Autoscaler configuration:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: streaming-processor-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: streaming-processor
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Pods
    pods:
      metric:
        name: kafka_consumer_lag
      target:
        type: AverageValue
        averageValue: "1000"
  - type: Pods
    pods:
      metric:
        name: event_processing_duration_ms_p95
      target:
        type: AverageValue
        averageValue: "100"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15
```

### Custom Auto-scaling Logic

Implement custom auto-scaling based on business metrics:

```python
from libs.streaming_analytics.monitoring import AutoScaler

class StreamingAutoScaler(AutoScaler):
    def __init__(self, config: MonitoringConfig):
        super().__init__(config)
        self.metrics = StreamingMetrics()
    
    async def evaluate_scaling_decision(self) -> dict:
        """Evaluate whether to scale up or down."""
        
        current_metrics = await self.get_current_metrics()
        
        # CPU and memory utilization
        cpu_usage = current_metrics.get("cpu_usage_percent", 0)
        memory_usage = current_metrics.get("memory_usage_percent", 0)
        
        # Business metrics
        consumer_lag = current_metrics.get("kafka_consumer_lag", 0)
        processing_latency_p95 = current_metrics.get("processing_latency_p95_ms", 0)
        error_rate = current_metrics.get("error_rate_percent", 0)
        
        # Scaling decision logic
        should_scale_up = (
            cpu_usage > 80 or
            memory_usage > 80 or
            consumer_lag > 5000 or
            processing_latency_p95 > 200
        )
        
        should_scale_down = (
            cpu_usage < 30 and
            memory_usage < 30 and
            consumer_lag < 100 and
            processing_latency_p95 < 50 and
            error_rate < 0.01
        )
        
        return {
            "should_scale_up": should_scale_up,
            "should_scale_down": should_scale_down,
            "current_metrics": current_metrics,
            "recommended_replicas": self.calculate_recommended_replicas(current_metrics)
        }
    
    def calculate_recommended_replicas(self, metrics: dict) -> int:
        """Calculate recommended number of replicas."""
        current_replicas = self.get_current_replicas()
        
        # Scale based on consumer lag
        if metrics.get("kafka_consumer_lag", 0) > 10000:
            return min(current_replicas * 2, self.config.max_instances)
        elif metrics.get("kafka_consumer_lag", 0) < 100:
            return max(current_replicas // 2, self.config.min_instances)
        
        return current_replicas
```

## Health Checks

### Component Health Checks

Implement health checks for all components:

```python
from libs.streaming_analytics.monitoring import HealthChecker

class StreamingHealthChecker(HealthChecker):
    def __init__(self):
        super().__init__()
        self.checks = {
            "kafka": self.check_kafka_health,
            "websocket": self.check_websocket_health,
            "ml_pipeline": self.check_ml_pipeline_health,
            "database": self.check_database_health
        }
    
    async def check_kafka_health(self) -> dict:
        """Check Kafka cluster health."""
        try:
            kafka_manager = KafkaManager()
            
            # Test producer
            test_topic = "health-check"
            await kafka_manager.produce_event(test_topic, {"test": "message"})
            
            # Test consumer
            consumer_lag = await kafka_manager.get_consumer_lag()
            
            is_healthy = consumer_lag < 1000  # Acceptable lag threshold
            
            return {
                "status": "healthy" if is_healthy else "degraded",
                "consumer_lag": consumer_lag,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def check_ml_pipeline_health(self) -> dict:
        """Check ML pipeline health."""
        try:
            pipeline = RealtimeMLPipeline()
            
            # Test prediction
            test_features = FeatureVector(features={"test": 1.0})
            test_request = PredictionRequest(
                model_name="health_check_model",
                features=test_features,
                timeout_ms=1000
            )
            
            result = await pipeline.predict_single(test_request)
            
            is_healthy = (
                result.status == PredictionStatus.SUCCESS and
                result.inference_time_ms < 100
            )
            
            return {
                "status": "healthy" if is_healthy else "unhealthy",
                "inference_time_ms": result.inference_time_ms,
                "models_cached": len(pipeline.model_cache),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
```

### Health Check Endpoint

Expose health checks via HTTP endpoint:

```python
from fastapi import FastAPI, HTTPException
from libs.streaming_analytics.monitoring import StreamingHealthChecker

app = FastAPI()
health_checker = StreamingHealthChecker()

@app.get("/health")
async def health_check():
    """Overall health check endpoint."""
    health_results = await health_checker.check_all()
    
    overall_status = "healthy"
    if any(result.get("status") == "unhealthy" for result in health_results.values()):
        overall_status = "unhealthy"
    elif any(result.get("status") == "degraded" for result in health_results.values()):
        overall_status = "degraded"
    
    response = {
        "status": overall_status,
        "timestamp": datetime.utcnow().isoformat(),
        "checks": health_results
    }
    
    if overall_status == "unhealthy":
        raise HTTPException(status_code=503, detail=response)
    
    return response

@app.get("/health/{component}")
async def component_health_check(component: str):
    """Individual component health check."""
    if component not in health_checker.checks:
        raise HTTPException(status_code=404, detail=f"Unknown component: {component}")
    
    result = await health_checker.check_component(component)
    
    if result.get("status") == "unhealthy":
        raise HTTPException(status_code=503, detail=result)
    
    return result
```

## SLA Monitoring

### Service Level Objectives

Define and monitor SLOs:

```python
from libs.streaming_analytics.monitoring import SLOMonitor

class StreamingSLOMonitor(SLOMonitor):
    def __init__(self):
        super().__init__()
        self.slos = {
            "event_processing_latency": {
                "target": 0.95,  # 95% of requests < 100ms
                "threshold_ms": 100,
                "measurement_window": "5m"
            },
            "prediction_accuracy": {
                "target": 0.90,  # 90% accuracy
                "measurement_window": "1h"
            },
            "uptime": {
                "target": 0.999,  # 99.9% uptime
                "measurement_window": "30d"
            },
            "throughput": {
                "target": 1000,  # Min 1000 events/second
                "measurement_window": "5m"
            }
        }
    
    async def calculate_slo_compliance(self, slo_name: str) -> dict:
        """Calculate SLO compliance for a specific SLO."""
        
        if slo_name not in self.slos:
            raise ValueError(f"Unknown SLO: {slo_name}")
        
        slo_config = self.slos[slo_name]
        
        if slo_name == "event_processing_latency":
            return await self.calculate_latency_slo(slo_config)
        elif slo_name == "uptime":
            return await self.calculate_uptime_slo(slo_config)
        elif slo_name == "throughput":
            return await self.calculate_throughput_slo(slo_config)
        
        # Add more SLO calculations as needed
    
    async def calculate_latency_slo(self, config: dict) -> dict:
        """Calculate latency SLO compliance."""
        
        # Query metrics for latency distribution
        query = f"""
        (
          sum(rate(event_processing_duration_ms_bucket{{le="{config['threshold_ms']}"}}[{config['measurement_window']}]))
          /
          sum(rate(event_processing_duration_ms_count[{config['measurement_window']}]))
        ) * 100
        """
        
        actual_compliance = await self.query_prometheus(query)
        target_compliance = config['target'] * 100
        
        is_compliant = actual_compliance >= target_compliance
        error_budget_remaining = (actual_compliance - target_compliance) / target_compliance
        
        return {
            "slo_name": "event_processing_latency",
            "target_compliance_percent": target_compliance,
            "actual_compliance_percent": actual_compliance,
            "is_compliant": is_compliant,
            "error_budget_remaining_percent": error_budget_remaining * 100,
            "measurement_window": config['measurement_window']
        }
```

### Error Budget Tracking

Track and alert on error budget consumption:

```python
class ErrorBudgetTracker:
    def __init__(self):
        self.metrics = StreamingMetrics()
    
    async def track_error_budget(self, slo_name: str, time_window: str = "30d"):
        """Track error budget consumption."""
        
        slo_monitor = StreamingSLOMonitor()
        compliance_data = await slo_monitor.calculate_slo_compliance(slo_name)
        
        error_budget_consumed = 100 - compliance_data['error_budget_remaining_percent']
        
        # Alert if error budget is being consumed too quickly
        if error_budget_consumed > 50:  # 50% of error budget consumed
            await self.send_error_budget_alert(slo_name, error_budget_consumed)
        
        # Record error budget metrics
        self.metrics.set_gauge(
            "slo_error_budget_consumed_percent",
            error_budget_consumed,
            tags={"slo": slo_name, "window": time_window}
        )
        
        return {
            "slo_name": slo_name,
            "error_budget_consumed_percent": error_budget_consumed,
            "days_until_budget_exhausted": self.calculate_days_until_exhaustion(
                error_budget_consumed, time_window
            )
        }
```

## Best Practices

### Metrics Collection
- Use consistent naming conventions
- Include relevant tags for filtering
- Avoid high-cardinality tags
- Set appropriate recording intervals

### Alerting
- Define clear runbooks for each alert
- Use proper alert severity levels
- Implement alert fatigue prevention
- Test alert channels regularly

### Dashboard Design
- Focus on actionable metrics
- Use appropriate visualization types
- Include SLO/SLA tracking
- Design for different audiences

### Performance Monitoring
- Monitor both technical and business metrics
- Track end-to-end request flows
- Set up proactive capacity planning
- Regular performance baseline updates