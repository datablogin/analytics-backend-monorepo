# Troubleshooting Guide

This guide covers common issues, debugging techniques, and solutions for the streaming analytics infrastructure.

## Overview

This troubleshooting guide is organized by component and provides:

- **Common Error Patterns**: Frequent issues and their symptoms
- **Diagnostic Tools**: Commands and techniques for investigation
- **Root Cause Analysis**: Systematic debugging approaches
- **Resolution Steps**: Step-by-step fix procedures
- **Prevention Strategies**: How to avoid recurring issues

## General Debugging Approach

### 1. Gather Information

```bash
# Check system resources
kubectl top pods -n streaming-analytics
kubectl top nodes

# Check pod status and events
kubectl get pods -n streaming-analytics -o wide
kubectl describe pods -n streaming-analytics

# Check logs
kubectl logs -n streaming-analytics deployment/websocket-server --tail=100
kubectl logs -n streaming-analytics deployment/stream-processor --tail=100
kubectl logs -n streaming-analytics deployment/ml-inference --tail=100
```

### 2. Check Health Endpoints

```bash
# Health check endpoints
curl http://websocket-service:8080/health
curl http://stream-processor-service:8080/health
curl http://ml-inference-service:8080/health

# Detailed metrics
curl http://websocket-service:8080/metrics
curl http://stream-processor-service:8080/stats
```

### 3. Monitor Key Metrics

```bash
# Prometheus queries for troubleshooting
# Event processing rate
rate(events_processed_total[5m])

# Error rate
rate(events_failed_total[5m]) / rate(events_processed_total[5m])

# Processing latency
histogram_quantile(0.95, rate(event_processing_duration_ms_bucket[5m]))

# Consumer lag
kafka_consumer_lag
```

## Kafka Issues

### High Consumer Lag

**Symptoms:**
- Events taking too long to process
- Real-time dashboards showing stale data
- Alerts about consumer lag

**Diagnosis:**
```bash
# Check consumer group status
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group streaming-processor-group --describe

# Check topic partition distribution
kafka-topics.sh --bootstrap-server kafka:9092 \
  --topic analytics-events --describe

# Monitor consumer metrics
curl http://stream-processor:8080/metrics | grep kafka_consumer_lag
```

**Common Causes and Solutions:**

1. **Insufficient Consumer Instances**
   ```bash
   # Scale up consumers
   kubectl scale deployment stream-processor --replicas=12 -n streaming-analytics
   
   # Check if scaling helps
   kubectl rollout status deployment/stream-processor -n streaming-analytics
   ```

2. **Slow Processing Logic**
   ```python
   # Add performance monitoring to identify bottlenecks
   import time
   from libs.streaming_analytics.monitoring import StreamingMetrics
   
   metrics = StreamingMetrics()
   
   async def process_event_with_monitoring(event: EventSchema):
       start_time = time.time()
       
       try:
           # Your processing logic
           result = await process_event(event)
           
           # Record success metrics
           processing_time = (time.time() - start_time) * 1000
           metrics.record_histogram("event_processing_duration_ms", processing_time)
           metrics.increment_counter("events_processed_success_total")
           
           return result
           
       except Exception as e:
           # Record error metrics
           processing_time = (time.time() - start_time) * 1000
           metrics.record_histogram("event_processing_duration_ms", processing_time)
           metrics.increment_counter("events_processed_error_total")
           
           logger.error("Event processing failed", 
                       event_id=event.event_id, error=str(e))
           raise
   ```

3. **Topic Partition Imbalance**
   ```bash
   # Check if partitions are evenly distributed
   kafka-topics.sh --bootstrap-server kafka:9092 \
     --topic analytics-events --describe
   
   # If needed, increase partitions (cannot decrease)
   kafka-topics.sh --bootstrap-server kafka:9092 \
     --topic analytics-events --alter --partitions 24
   ```

### Connection Issues

**Symptoms:**
- "Connection refused" errors
- Authentication failures
- Intermittent producer/consumer failures

**Diagnosis:**
```bash
# Test Kafka connectivity
kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# Check Kafka broker logs
kubectl logs kafka-0 -n kafka-system

# Test from within cluster
kubectl run kafka-test --image=confluentinc/cp-kafka:latest --rm -it --restart=Never \
  -- kafka-console-producer --broker-list kafka:9092 --topic test-topic
```

**Solutions:**

1. **DNS Resolution Issues**
   ```yaml
   # Add to pod spec if needed
   spec:
     dnsPolicy: ClusterFirst
     dnsConfig:
       options:
       - name: ndots
         value: "2"
       - name: edns0
   ```

2. **Network Policies Blocking Traffic**
   ```bash
   # Check network policies
   kubectl get networkpolicies -n streaming-analytics
   
   # Temporarily disable to test
   kubectl delete networkpolicy streaming-analytics-netpol -n streaming-analytics
   ```

3. **Authentication Configuration**
   ```bash
   # Verify secrets are correctly mounted
   kubectl get secret streaming-secrets -n streaming-analytics -o yaml
   
   # Check environment variables in pod
   kubectl exec -it deployment/stream-processor -n streaming-analytics -- env | grep KAFKA
   ```

## WebSocket Issues

### Connection Drops

**Symptoms:**
- Clients frequently disconnecting
- High connection churn in metrics
- "Connection reset by peer" errors

**Diagnosis:**
```bash
# Check WebSocket server logs
kubectl logs deployment/websocket-server -n streaming-analytics | grep -i "disconnect\|close\|error"

# Monitor connection metrics
curl http://websocket-service:8080/metrics | grep websocket_connections

# Check load balancer timeout settings
kubectl describe service websocket-service -n streaming-analytics
```

**Solutions:**

1. **Load Balancer Timeout Too Short**
   ```yaml
   # Update service annotation for AWS ALB
   apiVersion: v1
   kind: Service
   metadata:
     annotations:
       service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout: "300"
   ```

2. **Client-Side Keep-Alive Issues**
   ```javascript
   // Client-side: Implement proper ping/pong
   class RobustWebSocketClient {
     constructor(url) {
       this.url = url;
       this.pingInterval = null;
       this.reconnectAttempts = 0;
       this.maxReconnectAttempts = 5;
     }
     
     connect() {
       this.ws = new WebSocket(this.url);
       
       this.ws.onopen = () => {
         console.log('Connected');
         this.reconnectAttempts = 0;
         this.startPingInterval();
       };
       
       this.ws.onclose = (event) => {
         console.log('Disconnected:', event);
         this.stopPingInterval();
         this.attemptReconnect();
       };
       
       this.ws.onerror = (error) => {
         console.error('WebSocket error:', error);
       };
     }
     
     startPingInterval() {
       this.pingInterval = setInterval(() => {
         if (this.ws.readyState === WebSocket.OPEN) {
           this.ws.send(JSON.stringify({type: 'ping'}));
         }
       }, 30000); // 30 seconds
     }
     
     stopPingInterval() {
       if (this.pingInterval) {
         clearInterval(this.pingInterval);
         this.pingInterval = null;
       }
     }
     
     attemptReconnect() {
       if (this.reconnectAttempts < this.maxReconnectAttempts) {
         this.reconnectAttempts++;
         const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
         
         console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
         setTimeout(() => this.connect(), delay);
       }
     }
   }
   ```

3. **Server-Side Resource Limits**
   ```python
   # Increase connection limits and optimize cleanup
   class OptimizedWebSocketServer(WebSocketServer):
       def __init__(self, config):
           super().__init__(config)
           
           # Increase limits
           config.max_connections = 50000
           config.ping_interval = 20
           config.ping_timeout = 10
           
           # More aggressive cleanup
           self.cleanup_interval = 30  # seconds
       
       async def _cleanup_stale_connections(self):
           """More aggressive connection cleanup."""
           while self.is_running:
               try:
                   current_time = datetime.utcnow()
                   stale_clients = []
                   
                   for client_id, client in self.clients.items():
                       # Shorter timeout for stale detection
                       time_since_ping = (current_time - client.last_ping).total_seconds()
                       if time_since_ping > self.config.ping_timeout * 2:
                           stale_clients.append(client_id)
                   
                   # Cleanup in batches
                   for i in range(0, len(stale_clients), 10):
                       batch = stale_clients[i:i+10]
                       cleanup_tasks = [
                           self._disconnect_client(client_id, "Stale connection")
                           for client_id in batch
                       ]
                       await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                   
                   await asyncio.sleep(self.cleanup_interval)
                   
               except Exception as e:
                   logger.error("Cleanup error", error=str(e))
                   await asyncio.sleep(10)
   ```

### High Memory Usage

**Symptoms:**
- WebSocket server pods getting OOMKilled
- Increasing memory usage over time
- Slow response times

**Diagnosis:**
```bash
# Check memory usage
kubectl top pods -n streaming-analytics | grep websocket

# Get detailed memory metrics
kubectl exec deployment/websocket-server -n streaming-analytics -- \
  python -c "import psutil; print(f'Memory: {psutil.virtual_memory().percent}%')"

# Check for memory leaks in logs
kubectl logs deployment/websocket-server -n streaming-analytics | grep -i "memory\|leak\|oom"
```

**Solutions:**

1. **Connection State Cleanup**
   ```python
   # Implement proper cleanup in WebSocket server
   async def _disconnect_client(self, client_id: str, reason: str):
       """Enhanced client cleanup."""
       client = self.clients.pop(client_id, None)
       if not client:
           return
       
       # Clean up all subscriptions
       subscription_ids = list(client.subscriptions.keys())
       for sub_id in subscription_ids:
           self.subscriptions.pop(sub_id, None)
           
           # Remove from subscription index
           for subscription_type, client_set in self.subscription_index.items():
               client_set.discard(client_id)
       
       # Clear message queues
       self.message_queues.pop(client_id, None)
       
       # Close websocket connection
       if not client.websocket.closed:
           try:
               await client.websocket.close(reason=reason)
           except Exception:
               pass  # Connection might already be closed
       
       # Force garbage collection of client object
       del client
   ```

2. **Message Queue Limits**
   ```python
   # Implement bounded message queues
   from collections import deque
   
   class BoundedMessageQueue:
       def __init__(self, max_size: int = 1000):
           self.queue = deque(maxlen=max_size)
           self.dropped_messages = 0
       
       def append(self, message):
           if len(self.queue) >= self.queue.maxlen:
               self.dropped_messages += 1
               logger.warning("Message queue full, dropping message",
                            dropped_count=self.dropped_messages)
           
           self.queue.append(message)
   ```

## ML Inference Issues

### High Prediction Latency

**Symptoms:**
- ML predictions taking > 100ms
- Timeouts in prediction requests
- Users complaining about slow responses

**Diagnosis:**
```bash
# Check ML inference metrics
curl http://ml-inference:8080/metrics | grep ml_inference_duration

# Check model loading times
kubectl logs deployment/ml-inference -n streaming-analytics | grep "model.*loaded"

# Check resource utilization
kubectl top pods -n streaming-analytics | grep ml-inference
```

**Solutions:**

1. **Model Optimization**
   ```python
   # Optimize model for inference
   import joblib
   import time
   from libs.ml_models.optimization import ModelOptimizer
   
   class OptimizedModelLoader:
       def __init__(self):
           self.optimizer = ModelOptimizer()
       
       async def load_model(self, model_name: str, version: str):
           """Load and optimize model for inference."""
           start_time = time.time()
           
           # Load model from registry
           model = await self.registry.load_model(model_name, version)
           
           # Apply optimizations
           if hasattr(model, 'predict_proba'):
               # For sklearn models, use optimized prediction
               model = self.optimizer.optimize_sklearn_model(model)
           
           # Warm up model with dummy data
           await self.warm_up_model(model)
           
           load_time_ms = (time.time() - start_time) * 1000
           logger.info("Model loaded and optimized",
                      model=model_name, version=version, 
                      load_time_ms=load_time_ms)
           
           return model
       
       async def warm_up_model(self, model, num_samples: int = 100):
           """Warm up model with dummy predictions."""
           try:
               # Generate dummy features based on model input
               dummy_features = self.generate_dummy_features(model)
               
               # Make dummy predictions to warm up
               for _ in range(num_samples):
                   model.predict([dummy_features])
               
           except Exception as e:
               logger.warning("Model warm-up failed", error=str(e))
   ```

2. **Batch Processing Optimization**
   ```python
   # Implement adaptive batching
   class AdaptiveBatchProcessor:
       def __init__(self):
           self.current_batch_size = 32
           self.min_batch_size = 1
           self.max_batch_size = 128
           self.target_latency_ms = 50
           self.latency_history = deque(maxlen=100)
       
       def adjust_batch_size(self, actual_latency_ms: float):
           """Dynamically adjust batch size based on latency."""
           self.latency_history.append(actual_latency_ms)
           
           if len(self.latency_history) < 10:
               return  # Need more data
           
           avg_latency = sum(self.latency_history) / len(self.latency_history)
           
           if avg_latency > self.target_latency_ms * 1.2:
               # Too slow, reduce batch size
               self.current_batch_size = max(
                   self.min_batch_size,
                   int(self.current_batch_size * 0.8)
               )
           elif avg_latency < self.target_latency_ms * 0.8:
               # Fast enough, can increase batch size
               self.current_batch_size = min(
                   self.max_batch_size,
                   int(self.current_batch_size * 1.2)
               )
           
           logger.debug("Adjusted batch size",
                       new_size=self.current_batch_size,
                       avg_latency_ms=avg_latency)
   ```

### Model Loading Failures

**Symptoms:**
- "Model not found" errors
- "Failed to load model" in logs
- ML inference service showing unhealthy

**Diagnosis:**
```bash
# Check MLflow connectivity
kubectl exec deployment/ml-inference -n streaming-analytics -- \
  python -c "
import mlflow
try:
    client = mlflow.tracking.MlflowClient()
    experiments = client.list_experiments()
    print(f'Found {len(experiments)} experiments')
except Exception as e:
    print(f'MLflow error: {e}')
"

# Check model registry access
kubectl logs deployment/ml-inference -n streaming-analytics | grep -i "mlflow\|model.*registry\|authentication"
```

**Solutions:**

1. **Connection Issues**
   ```python
   # Implement robust MLflow connectivity
   import mlflow
   import time
   from typing import Optional
   
   class RobustMLflowClient:
       def __init__(self, tracking_uri: str, max_retries: int = 3):
           self.tracking_uri = tracking_uri
           self.max_retries = max_retries
           self.client: Optional[mlflow.tracking.MlflowClient] = None
       
       async def get_client(self) -> mlflow.tracking.MlflowClient:
           """Get MLflow client with retry logic."""
           if self.client is None:
               await self._initialize_client()
           
           return self.client
       
       async def _initialize_client(self):
           """Initialize MLflow client with retries."""
           last_error = None
           
           for attempt in range(self.max_retries):
               try:
                   mlflow.set_tracking_uri(self.tracking_uri)
                   self.client = mlflow.tracking.MlflowClient()
                   
                   # Test connection
                   experiments = self.client.list_experiments()
                   logger.info("MLflow client initialized",
                             experiments_count=len(experiments))
                   return
                   
               except Exception as e:
                   last_error = e
                   wait_time = 2 ** attempt
                   logger.warning("MLflow connection failed",
                                attempt=attempt + 1, 
                                error=str(e),
                                retry_in_seconds=wait_time)
                   
                   if attempt < self.max_retries - 1:
                       await asyncio.sleep(wait_time)
           
           raise Exception(f"Failed to connect to MLflow after {self.max_retries} attempts: {last_error}")
   ```

2. **Model Caching Issues**
   ```python
   # Implement persistent model cache
   import os
   import pickle
   import hashlib
   
   class PersistentModelCache:
       def __init__(self, cache_dir: str = "/tmp/model_cache"):
           self.cache_dir = cache_dir
           os.makedirs(cache_dir, exist_ok=True)
       
       def _get_cache_key(self, model_name: str, version: str) -> str:
           """Generate cache key for model."""
           key_string = f"{model_name}:{version}"
           return hashlib.md5(key_string.encode()).hexdigest()
       
       def _get_cache_path(self, cache_key: str) -> str:
           """Get file path for cached model."""
           return os.path.join(self.cache_dir, f"{cache_key}.pkl")
       
       async def get_cached_model(self, model_name: str, version: str):
           """Get model from persistent cache."""
           cache_key = self._get_cache_key(model_name, version)
           cache_path = self._get_cache_path(cache_key)
           
           if os.path.exists(cache_path):
               try:
                   with open(cache_path, 'rb') as f:
                       model = pickle.load(f)
                   
                   logger.info("Model loaded from cache",
                             model=model_name, version=version)
                   return model
                   
               except Exception as e:
                   logger.warning("Failed to load cached model",
                                model=model_name, error=str(e))
                   # Remove corrupted cache file
                   os.remove(cache_path)
           
           return None
       
       async def cache_model(self, model_name: str, version: str, model):
           """Save model to persistent cache."""
           cache_key = self._get_cache_key(model_name, version)
           cache_path = self._get_cache_path(cache_key)
           
           try:
               with open(cache_path, 'wb') as f:
                   pickle.dump(model, f)
               
               logger.info("Model cached successfully",
                         model=model_name, version=version)
               
           except Exception as e:
               logger.error("Failed to cache model",
                          model=model_name, error=str(e))
   ```

## Database Issues

### Connection Pool Exhaustion

**Symptoms:**
- "Connection pool exhausted" errors
- "Could not connect to database" errors
- High database connection count

**Diagnosis:**
```bash
# Check database connections
kubectl exec deployment/stream-processor -n streaming-analytics -- \
  python -c "
from libs.analytics_core.database import get_db_engine
engine = get_db_engine()
print(f'Pool size: {engine.pool.size()}')
print(f'Checked out: {engine.pool.checkedout()}')
print(f'Overflow: {engine.pool.overflow()}')
"

# Check PostgreSQL connection count
psql -h your-db-host -U username -c "
SELECT count(*) as total_connections, 
       state, 
       application_name 
FROM pg_stat_activity 
GROUP BY state, application_name;
"
```

**Solutions:**

1. **Optimize Connection Pool Settings**
   ```python
   # Adjust connection pool configuration
   from sqlalchemy import create_engine
   from sqlalchemy.pool import QueuePool
   
   def create_optimized_engine(database_url: str):
       return create_engine(
           database_url,
           poolclass=QueuePool,
           
           # Increase pool size based on workload
           pool_size=30,              # Base connections
           max_overflow=50,           # Additional connections
           
           # Faster connection recycling
           pool_recycle=1800,         # 30 minutes
           pool_timeout=20,           # Shorter timeout
           
           # Connection validation
           pool_pre_ping=True,
           
           # Optimize connection parameters
           connect_args={
               "keepalives_idle": "300",    # 5 minutes
               "keepalives_interval": "30", # 30 seconds
               "keepalives_count": "3",     # 3 retries
               "tcp_user_timeout": "30000", # 30 seconds
           }
       )
   ```

2. **Implement Connection Retry Logic**
   ```python
   import asyncio
   from sqlalchemy.exc import DisconnectionError, OperationalError
   
   class RobustDatabaseClient:
       def __init__(self, engine):
           self.engine = engine
           self.max_retries = 3
           self.base_delay = 1.0
       
       async def execute_with_retry(self, query, params=None):
           """Execute query with retry logic."""
           last_error = None
           
           for attempt in range(self.max_retries):
               try:
                   async with self.engine.begin() as conn:
                       result = await conn.execute(query, params or {})
                       return result
                       
               except (DisconnectionError, OperationalError) as e:
                   last_error = e
                   
                   if attempt < self.max_retries - 1:
                       delay = self.base_delay * (2 ** attempt)
                       logger.warning("Database query failed, retrying",
                                    attempt=attempt + 1,
                                    error=str(e),
                                    retry_delay=delay)
                       await asyncio.sleep(delay)
                   else:
                       logger.error("Database query failed after all retries",
                                  error=str(e))
                       raise
               
               except Exception as e:
                   # Don't retry for non-connection errors
                   logger.error("Database query error", error=str(e))
                   raise
           
           raise last_error
   ```

### Slow Queries

**Symptoms:**
- Database queries taking > 5 seconds
- High database CPU usage
- Query timeouts in application logs

**Diagnosis:**
```sql
-- Find slow queries in PostgreSQL
SELECT query,
       mean_time,
       calls,
       total_time,
       mean_time * calls as total_time_calc
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 20;

-- Check for missing indexes
SELECT schemaname,
       tablename,
       attname,
       n_distinct,
       correlation
FROM pg_stats
WHERE schemaname = 'public'
  AND n_distinct > 100
  AND correlation < 0.1;

-- Check for table bloat
SELECT schemaname,
       tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

**Solutions:**

1. **Add Missing Indexes**
   ```sql
   -- Common indexes for streaming analytics
   
   -- Events table indexes
   CREATE INDEX CONCURRENTLY idx_events_timestamp_type 
   ON events (timestamp, event_type);
   
   CREATE INDEX CONCURRENTLY idx_events_user_timestamp 
   ON events (user_id, timestamp);
   
   CREATE INDEX CONCURRENTLY idx_events_payload_gin 
   ON events USING gin (payload);
   
   -- Time-based partitioning for large tables
   CREATE TABLE events_2024_01 PARTITION OF events
   FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
   ```

2. **Query Optimization**
   ```python
   # Optimize queries with proper limits and indexes
   async def get_recent_events_optimized(
       self,
       event_types: list[str],
       hours_back: int = 1,
       limit: int = 1000
   ):
       """Optimized query with proper indexing."""
       
       query = text("""
       SELECT event_id, event_type, user_id, timestamp, payload
       FROM events
       WHERE timestamp >= :start_time
         AND event_type = ANY(:event_types)
       ORDER BY timestamp DESC
       LIMIT :limit
       """)
       
       # Use index on (timestamp, event_type)
       params = {
           "start_time": datetime.utcnow() - timedelta(hours=hours_back),
           "event_types": event_types,
           "limit": limit
       }
       
       return await self.execute_with_retry(query, params)
   ```

## Performance Issues

### High CPU Usage

**Symptoms:**
- Pods hitting CPU limits
- Slow response times
- CPU throttling in metrics

**Diagnosis:**
```bash
# Check CPU usage by pod
kubectl top pods -n streaming-analytics --sort-by=cpu

# Check CPU throttling
kubectl exec deployment/stream-processor -n streaming-analytics -- \
  cat /sys/fs/cgroup/cpu/cpu.stat | grep throttled

# Profile Python code
kubectl exec deployment/stream-processor -n streaming-analytics -- \
  python -m cProfile -o profile.stats -m your_module
```

**Solutions:**

1. **Optimize Processing Logic**
   ```python
   # Use vectorized operations
   import numpy as np
   import pandas as pd
   
   class OptimizedEventProcessor:
       def process_events_batch(self, events: list[EventSchema]) -> dict:
           """Process events using vectorized operations."""
           
           # Convert to DataFrame for vectorized operations
           df = pd.DataFrame([
               {
                   'user_id': event.user_id,
                   'event_type': event.event_type,
                   'value': event.payload.get('value', 0),
                   'timestamp': event.timestamp
               }
               for event in events
           ])
           
           # Vectorized aggregations
           result = {
               'total_events': len(df),
               'unique_users': df['user_id'].nunique(),
               'avg_value': df['value'].mean(),
               'sum_value': df['value'].sum(),
               'events_by_type': df['event_type'].value_counts().to_dict()
           }
           
           return result
   ```

2. **Implement Caching**
   ```python
   # Add caching to expensive operations
   from functools import lru_cache
   import asyncio
   
   class CachedEventProcessor:
       def __init__(self):
           self.cache = {}
           self.cache_ttl = 300  # 5 minutes
       
       @lru_cache(maxsize=1000)
       def get_user_segment(self, user_id: str) -> str:
           """Cached user segment lookup."""
           # Expensive database lookup
           return self._fetch_user_segment(user_id)
       
       async def get_cached_aggregation(
           self, 
           key: str, 
           compute_func,
           ttl: int = 300
       ):
           """Generic async cache with TTL."""
           import time
           
           now = time.time()
           
           if key in self.cache:
               value, timestamp = self.cache[key]
               if now - timestamp < ttl:
                   return value
           
           # Compute new value
           value = await compute_func()
           self.cache[key] = (value, now)
           
           return value
   ```

### Memory Leaks

**Symptoms:**
- Steadily increasing memory usage
- Pods getting OOMKilled
- Garbage collection taking too long

**Diagnosis:**
```python
# Memory profiling script
import psutil
import gc
import sys
from pympler import tracker, muppy, summary

def diagnose_memory_usage():
    """Diagnose memory usage and potential leaks."""
    
    # Basic memory info
    process = psutil.Process()
    memory_info = process.memory_info()
    
    print(f"RSS: {memory_info.rss / 1024 / 1024:.2f} MB")
    print(f"VMS: {memory_info.vms / 1024 / 1024:.2f} MB")
    
    # Garbage collection info
    print(f"GC counts: {gc.get_count()}")
    print(f"GC threshold: {gc.get_threshold()}")
    
    # Top memory consuming objects
    all_objects = muppy.get_objects()
    sum1 = summary.summarize(all_objects)
    summary.print_(sum1, limit=15)
    
    # Track memory growth
    tr = tracker.SummaryTracker()
    # ... run some code ...
    tr.print_diff()

# Usage in your application
if __name__ == "__main__":
    diagnose_memory_usage()
```

**Solutions:**

1. **Fix Common Memory Leaks**
   ```python
   # Avoid circular references
   import weakref
   
   class EventProcessor:
       def __init__(self):
           # Use weak references for callbacks
           self.callbacks = weakref.WeakSet()
           
           # Limit collection sizes
           self.recent_events = deque(maxlen=10000)
           
           # Clean up resources properly
           self._cleanup_tasks = []
       
       def add_callback(self, callback):
           """Add callback with weak reference."""
           self.callbacks.add(callback)
       
       async def cleanup(self):
           """Proper cleanup of resources."""
           # Cancel all tasks
           for task in self._cleanup_tasks:
               task.cancel()
           
           # Clear collections
           self.recent_events.clear()
           self.callbacks.clear()
           
           # Force garbage collection
           gc.collect()
   ```

2. **Implement Memory Monitoring**
   ```python
   # Memory monitoring decorator
   import functools
   import psutil
   import os
   
   def monitor_memory(func):
       """Decorator to monitor memory usage of functions."""
       
       @functools.wraps(func)
       async def wrapper(*args, **kwargs):
           process = psutil.Process(os.getpid())
           
           # Memory before
           mem_before = process.memory_info().rss / 1024 / 1024
           
           try:
               result = await func(*args, **kwargs)
               return result
           finally:
               # Memory after
               mem_after = process.memory_info().rss / 1024 / 1024
               mem_diff = mem_after - mem_before
               
               if mem_diff > 10:  # More than 10MB increase
                   logger.warning("High memory usage detected",
                                function=func.__name__,
                                memory_before_mb=mem_before,
                                memory_after_mb=mem_after,
                                memory_increase_mb=mem_diff)
       
       return wrapper
   
   # Usage
   @monitor_memory
   async def process_large_batch(events):
       # Your processing logic
       pass
   ```

## Emergency Procedures

### System Recovery

#### Complete System Restart

```bash
#!/bin/bash
# emergency_restart.sh

echo "Starting emergency system restart..."

# 1. Scale down processing components
kubectl scale deployment stream-processor --replicas=0 -n streaming-analytics
kubectl scale deployment ml-inference --replicas=0 -n streaming-analytics
kubectl scale deployment websocket-server --replicas=0 -n streaming-analytics

# 2. Wait for graceful shutdown
sleep 30

# 3. Clear any stuck resources
kubectl delete pods -l app=stream-processor -n streaming-analytics --force --grace-period=0
kubectl delete pods -l app=ml-inference -n streaming-analytics --force --grace-period=0
kubectl delete pods -l app=websocket-server -n streaming-analytics --force --grace-period=0

# 4. Scale back up with health checks
kubectl scale deployment stream-processor --replicas=6 -n streaming-analytics
kubectl rollout status deployment/stream-processor -n streaming-analytics --timeout=300s

kubectl scale deployment ml-inference --replicas=4 -n streaming-analytics
kubectl rollout status deployment/ml-inference -n streaming-analytics --timeout=300s

kubectl scale deployment websocket-server --replicas=3 -n streaming-analytics
kubectl rollout status deployment/websocket-server -n streaming-analytics --timeout=300s

echo "Emergency restart completed"
```

#### Data Recovery

```bash
#!/bin/bash
# data_recovery.sh

echo "Starting data recovery process..."

# 1. Check Kafka topic integrity
kafka-topics.sh --bootstrap-server kafka:9092 --list
kafka-log-dirs.sh --bootstrap-server kafka:9092 --describe

# 2. Reset consumer groups if needed (CAUTION: Will replay all messages)
read -p "Reset consumer groups? This will replay all messages. (y/N): " confirm
if [[ $confirm == [yY] ]]; then
    kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
      --group streaming-processor-group --reset-offsets --to-earliest --all-topics --execute
fi

# 3. Verify database integrity
kubectl exec deployment/stream-processor -n streaming-analytics -- \
  python -c "
from libs.analytics_core.database import get_db_engine
engine = get_db_engine()
with engine.connect() as conn:
    result = conn.execute('SELECT COUNT(*) FROM events WHERE timestamp > NOW() - INTERVAL \"1 hour\"')
    print(f'Events in last hour: {result.scalar()}')
"

echo "Data recovery completed"
```

## Monitoring and Alerting

### Key Alerts to Set Up

```yaml
# prometheus-alerts.yaml
groups:
- name: streaming-analytics-critical
  rules:
  - alert: HighErrorRate
    expr: rate(events_failed_total[5m]) / rate(events_processed_total[5m]) > 0.05
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "High error rate in event processing"
      description: "Error rate is {{ $value | humanizePercentage }}"
      runbook_url: "https://docs.company.com/runbook/high-error-rate"

  - alert: ConsumerLagHigh
    expr: kafka_consumer_lag > 10000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Kafka consumer lag is high"
      description: "Consumer lag is {{ $value }} messages"

  - alert: MLInferenceLatencyHigh
    expr: histogram_quantile(0.95, rate(ml_inference_duration_ms_bucket[5m])) > 100
    for: 3m
    labels:
      severity: warning
    annotations:
      summary: "ML inference latency is high"
      description: "95th percentile latency is {{ $value }}ms"

  - alert: WebSocketConnectionsDropping
    expr: rate(websocket_disconnections_total[5m]) > 10
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High WebSocket disconnection rate"
      description: "{{ $value }} disconnections per second"
```

### Health Check Scripts

```python
#!/usr/bin/env python3
# health_check.py

import asyncio
import aiohttp
import sys
from typing import Dict, List

class HealthChecker:
    def __init__(self):
        self.services = {
            "websocket-server": "http://websocket-service:8080/health",
            "stream-processor": "http://stream-processor-service:8080/health", 
            "ml-inference": "http://ml-inference-service:8080/health"
        }
        self.timeout = 5
    
    async def check_service(self, name: str, url: str) -> Dict:
        """Check individual service health."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=self.timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {"service": name, "status": "healthy", "details": data}
                    else:
                        return {"service": name, "status": "unhealthy", 
                               "error": f"HTTP {response.status}"}
        
        except asyncio.TimeoutError:
            return {"service": name, "status": "unhealthy", "error": "timeout"}
        except Exception as e:
            return {"service": name, "status": "unhealthy", "error": str(e)}
    
    async def check_all_services(self) -> List[Dict]:
        """Check all services concurrently."""
        tasks = [
            self.check_service(name, url) 
            for name, url in self.services.items()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                service_name = list(self.services.keys())[i]
                final_results.append({
                    "service": service_name,
                    "status": "unhealthy", 
                    "error": str(result)
                })
            else:
                final_results.append(result)
        
        return final_results
    
    async def run_health_check(self) -> bool:
        """Run health check and return overall status."""
        results = await self.check_all_services()
        
        all_healthy = True
        for result in results:
            status = result["status"]
            service = result["service"]
            
            if status == "healthy":
                print(f"✅ {service}: {status}")
            else:
                print(f"❌ {service}: {status} - {result.get('error', '')}")
                all_healthy = False
        
        return all_healthy

async def main():
    checker = HealthChecker()
    is_healthy = await checker.run_health_check()
    
    if is_healthy:
        print("\n🎉 All services are healthy!")
        sys.exit(0)
    else:
        print("\n⚠️  Some services are unhealthy!")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
```

## Best Practices

### Debugging Mindset
1. **Start with metrics** - Look at dashboards first
2. **Check recent changes** - Deployments, config changes
3. **Follow the data flow** - Trace requests end-to-end
4. **Use structured logging** - Make logs searchable
5. **Document solutions** - Build a knowledge base

### Prevention Strategies
1. **Comprehensive monitoring** - Monitor everything that matters
2. **Gradual rollouts** - Use canary deployments
3. **Load testing** - Test before production load hits
4. **Capacity planning** - Monitor trends and scale proactively
5. **Regular maintenance** - Keep dependencies updated

### Emergency Response
1. **Have runbooks** - Document common procedures
2. **Practice recovery** - Regular disaster recovery drills
3. **Monitor during incidents** - Don't make things worse
4. **Post-incident reviews** - Learn from every incident
5. **Automate when possible** - Reduce human error