# Performance Tuning Guide

This guide covers optimizing the streaming analytics infrastructure for maximum throughput, minimal latency, and efficient resource utilization.

## Overview

The streaming analytics system is designed to meet these performance targets:

- **Throughput**: 100,000+ events/second
- **Latency**: < 100ms end-to-end processing
- **ML Inference**: < 50ms prediction latency
- **WebSocket**: < 10ms message delivery
- **Availability**: 99.9% uptime

## Performance Architecture

```
Input → Partitioning → Parallel Processing → Output
  ↓         ↓              ↓               ↓
Kafka    Consumers     Stream Proc      WebSocket
Events   (Parallel)   (Windows)       (Broadcast)
```

## Kafka Performance Tuning

### Producer Optimization

#### High Throughput Configuration

```python
high_throughput_producer_config = {
    # Batching settings
    "batch_size": 65536,           # 64KB batches
    "linger_ms": 10,               # Wait 10ms for batching
    "buffer_memory": 134217728,    # 128MB buffer
    
    # Compression
    "compression_type": "lz4",     # Fast compression
    
    # Acknowledgments  
    "acks": "1",                   # Leader ack only (vs "all")
    "enable_idempotence": False,   # Disable for higher throughput
    
    # Retries
    "retries": 3,
    "retry_backoff_ms": 100,
    
    # Network
    "send_buffer_bytes": 131072,   # 128KB
    "receive_buffer_bytes": 65536, # 64KB
}
```

#### Low Latency Configuration

```python
low_latency_producer_config = {
    # Minimal batching
    "batch_size": 1,               # No batching
    "linger_ms": 0,                # Send immediately
    
    # No compression overhead
    "compression_type": "none",
    
    # Fast acknowledgments
    "acks": "1",
    "retries": 0,                  # No retries for lowest latency
    
    # Network optimization
    "tcp_nagle": False,            # Disable Nagle's algorithm
}
```

### Consumer Optimization

#### High Throughput Consumer

```python
high_throughput_consumer_config = {
    # Fetch settings
    "fetch_min_bytes": 65536,      # Wait for 64KB
    "fetch_max_wait_ms": 500,      # Max wait 500ms
    "max_poll_records": 5000,      # Process 5k records per poll
    
    # Buffer sizes
    "receive_buffer_bytes": 262144, # 256KB
    "send_buffer_bytes": 131072,   # 128KB
    
    # Offset management
    "enable_auto_commit": False,   # Manual commits for batching
    "auto_commit_interval_ms": 1000,
    
    # Session management
    "session_timeout_ms": 45000,   # Longer timeout
    "heartbeat_interval_ms": 15000,
    
    # Partition assignment
    "partition_assignment_strategy": "RoundRobinAssignor",
}
```

#### Low Latency Consumer

```python
low_latency_consumer_config = {
    # Minimal fetch settings
    "fetch_min_bytes": 1,          # Don't wait for data
    "fetch_max_wait_ms": 0,        # Return immediately
    "max_poll_records": 100,       # Process fewer records
    
    # Fast offset commits
    "enable_auto_commit": True,
    "auto_commit_interval_ms": 100,
    
    # Shorter timeouts
    "session_timeout_ms": 10000,
    "heartbeat_interval_ms": 3000,
}
```

### Topic Configuration

#### Partition Strategy

```python
def calculate_optimal_partitions(
    target_throughput_mb_per_sec: float,
    target_consumer_count: int,
    max_partition_throughput_mb: float = 10.0
) -> int:
    """Calculate optimal number of partitions."""
    
    # Based on throughput requirements
    throughput_partitions = math.ceil(
        target_throughput_mb_per_sec / max_partition_throughput_mb
    )
    
    # Based on parallelism requirements
    parallelism_partitions = target_consumer_count
    
    # Take the maximum, ensure it's a power of 2 for even distribution
    optimal_partitions = max(throughput_partitions, parallelism_partitions)
    
    # Round up to next power of 2
    return 2 ** math.ceil(math.log2(optimal_partitions))

# Example usage
partitions = calculate_optimal_partitions(
    target_throughput_mb_per_sec=50,  # 50 MB/s
    target_consumer_count=12          # 12 consumers
)
# Result: 16 partitions
```

#### Replication and Durability

```python
# High throughput topic config
high_throughput_topic_config = {
    "cleanup.policy": "delete",
    "retention.ms": "86400000",        # 1 day (vs 7 days)
    "segment.ms": "3600000",           # 1 hour segments
    "compression.type": "lz4",         # Fast compression
    "min.insync.replicas": "1",        # Relaxed durability
    "unclean.leader.election.enable": "true",  # Allow some data loss
}

# High durability topic config
high_durability_topic_config = {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",       # 7 days
    "segment.ms": "86400000",          # 1 day segments
    "compression.type": "gzip",        # Better compression
    "min.insync.replicas": "2",        # Strong durability
    "unclean.leader.election.enable": "false",
}
```

## Stream Processing Optimization

### Window Processing

#### Tumbling Window Optimization

```python
from libs.streaming_analytics import WindowedProcessor, WindowType

class OptimizedTumblingProcessor(WindowedProcessor):
    def __init__(self):
        # Optimize for high throughput
        config = {
            "window_size_ms": 60000,       # 1-minute windows
            "parallelism": 16,             # High parallelism
            "max_batch_size": 10000,       # Large batches
            "buffer_timeout_ms": 100,      # Quick buffer flush
            "enable_early_trigger": True,  # Process partial windows
        }
        super().__init__(WindowType.TUMBLING, config)
    
    async def process_window(self, events: list[EventSchema]) -> dict:
        """Optimized window processing."""
        
        # Use vectorized operations
        import numpy as np
        
        # Extract numeric values efficiently
        values = np.array([
            event.payload.get("value", 0) 
            for event in events
        ])
        
        # Compute aggregations in batch
        return {
            "count": len(values),
            "sum": np.sum(values),
            "mean": np.mean(values),
            "std": np.std(values),
            "min": np.min(values),
            "max": np.max(values),
            "window_start": self.current_window_start,
            "window_end": self.current_window_end
        }
```

#### Sliding Window Memory Optimization

```python
from collections import deque
from typing import Deque

class MemoryOptimizedSlidingWindow:
    def __init__(self, window_size_ms: int, slide_interval_ms: int):
        self.window_size_ms = window_size_ms
        self.slide_interval_ms = slide_interval_ms
        
        # Use deque for efficient append/pop
        self.events: Deque[EventSchema] = deque()
        self.aggregates = {}
        
        # Pre-compute incremental aggregates
        self.sum_value = 0.0
        self.count = 0
        self.min_heap = []  # For min tracking
        self.max_heap = []  # For max tracking
    
    def add_event(self, event: EventSchema) -> None:
        """Add event with incremental aggregate updates."""
        current_time = time.time() * 1000
        value = event.payload.get("value", 0)
        
        # Add to window
        self.events.append(event)
        
        # Update incremental aggregates
        self.sum_value += value
        self.count += 1
        heapq.heappush(self.min_heap, value)
        heapq.heappush(self.max_heap, -value)  # Negative for max heap
        
        # Remove expired events
        self.evict_expired_events(current_time)
    
    def evict_expired_events(self, current_time: float) -> None:
        """Efficiently remove expired events."""
        cutoff_time = current_time - self.window_size_ms
        
        while (self.events and 
               self.events[0].timestamp.timestamp() * 1000 < cutoff_time):
            
            expired_event = self.events.popleft()
            expired_value = expired_event.payload.get("value", 0)
            
            # Update incremental aggregates
            self.sum_value -= expired_value
            self.count -= 1
            
            # Note: heap cleanup is done lazily for performance
    
    def get_current_aggregates(self) -> dict:
        """Get current window aggregates efficiently."""
        if self.count == 0:
            return {"count": 0, "sum": 0, "mean": 0}
        
        return {
            "count": self.count,
            "sum": self.sum_value,
            "mean": self.sum_value / self.count,
            "window_size_ms": self.window_size_ms
        }
```

### Parallel Processing

#### Worker Pool Optimization

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

class OptimizedStreamProcessor:
    def __init__(self):
        # CPU-bound work: use process pool
        self.cpu_workers = min(32, multiprocessing.cpu_count() * 2)
        self.process_pool = ProcessPoolExecutor(max_workers=self.cpu_workers)
        
        # I/O-bound work: use thread pool
        self.io_workers = min(100, multiprocessing.cpu_count() * 8)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.io_workers)
        
        # Async semaphore for rate limiting
        self.semaphore = asyncio.Semaphore(1000)
    
    async def process_events_batch(self, events: list[EventSchema]) -> list:
        """Process events in optimized batches."""
        
        # Split into CPU and I/O bound tasks
        cpu_intensive_events = []
        io_intensive_events = []
        
        for event in events:
            if self.is_cpu_intensive(event):
                cpu_intensive_events.append(event)
            else:
                io_intensive_events.append(event)
        
        # Process CPU-intensive work in process pool
        cpu_tasks = []
        if cpu_intensive_events:
            # Batch CPU work to reduce overhead
            batch_size = max(1, len(cpu_intensive_events) // self.cpu_workers)
            
            for i in range(0, len(cpu_intensive_events), batch_size):
                batch = cpu_intensive_events[i:i + batch_size]
                task = asyncio.get_event_loop().run_in_executor(
                    self.process_pool,
                    self.process_cpu_intensive_batch,
                    batch
                )
                cpu_tasks.append(task)
        
        # Process I/O-intensive work in thread pool
        io_tasks = []
        for event in io_intensive_events:
            async with self.semaphore:
                task = asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    self.process_io_intensive_event,
                    event
                )
                io_tasks.append(task)
        
        # Wait for all tasks
        cpu_results = await asyncio.gather(*cpu_tasks, return_exceptions=True)
        io_results = await asyncio.gather(*io_tasks, return_exceptions=True)
        
        # Combine results
        all_results = []
        for result_batch in cpu_results:
            if isinstance(result_batch, list):
                all_results.extend(result_batch)
        
        for result in io_results:
            if not isinstance(result, Exception):
                all_results.append(result)
        
        return all_results
```

## ML Inference Optimization

### Model Caching

#### Smart Model Cache

```python
from collections import OrderedDict
import asyncio
import time
from typing import Optional

class SmartModelCache:
    def __init__(self, max_size: int = 10, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        
        # LRU cache with timestamps
        self.cache: OrderedDict = OrderedDict()
        self.timestamps: dict = {}
        self.access_counts: dict = {}
        self.lock = asyncio.Lock()
    
    async def get_model(self, model_name: str, version: str) -> Optional[Any]:
        """Get model with smart eviction."""
        async with self.lock:
            cache_key = f"{model_name}:{version}"
            current_time = time.time()
            
            # Check if model exists and is not expired
            if cache_key in self.cache:
                if current_time - self.timestamps[cache_key] < self.ttl_seconds:
                    # Move to end (most recently used)
                    self.cache.move_to_end(cache_key)
                    self.access_counts[cache_key] += 1
                    return self.cache[cache_key]
                else:
                    # Expired, remove
                    self.cache.pop(cache_key)
                    self.timestamps.pop(cache_key)
                    self.access_counts.pop(cache_key)
            
            return None
    
    async def put_model(
        self, 
        model_name: str, 
        version: str, 
        model: Any,
        load_time_ms: float
    ) -> None:
        """Cache model with smart eviction."""
        async with self.lock:
            cache_key = f"{model_name}:{version}"
            current_time = time.time()
            
            # Evict if cache is full
            while len(self.cache) >= self.max_size:
                await self._evict_least_valuable()
            
            # Add new model
            self.cache[cache_key] = model
            self.timestamps[cache_key] = current_time
            self.access_counts[cache_key] = 1
            
            # Add metadata for smart eviction
            model._cache_metadata = {
                "load_time_ms": load_time_ms,
                "size_mb": self._estimate_model_size(model),
                "cache_time": current_time
            }
    
    async def _evict_least_valuable(self) -> None:
        """Evict least valuable model based on multiple factors."""
        if not self.cache:
            return
        
        current_time = time.time()
        min_value = float('inf')
        evict_key = None
        
        for cache_key, model in self.cache.items():
            # Calculate value score
            access_count = self.access_counts[cache_key]
            age_seconds = current_time - self.timestamps[cache_key]
            
            # Factors: access frequency, recency, load time, size
            metadata = getattr(model, '_cache_metadata', {})
            load_time_ms = metadata.get('load_time_ms', 1000)
            size_mb = metadata.get('size_mb', 100)
            
            # Higher access count and load time = higher value
            # Higher age and size = lower value
            value_score = (access_count * load_time_ms) / (age_seconds * size_mb)
            
            if value_score < min_value:
                min_value = value_score
                evict_key = cache_key
        
        # Evict least valuable model
        if evict_key:
            self.cache.pop(evict_key)
            self.timestamps.pop(evict_key)
            self.access_counts.pop(evict_key)
    
    def _estimate_model_size(self, model: Any) -> float:
        """Estimate model size in MB."""
        try:
            import sys
            return sys.getsizeof(model) / (1024 * 1024)
        except:
            return 100.0  # Default estimate
```

### Batch Inference Optimization

#### Dynamic Batching

```python
import asyncio
from collections import deque
import time

class DynamicBatchInference:
    def __init__(
        self, 
        max_batch_size: int = 32,
        max_wait_time_ms: int = 50,
        target_latency_ms: int = 30
    ):
        self.max_batch_size = max_batch_size
        self.max_wait_time_ms = max_wait_time_ms
        self.target_latency_ms = target_latency_ms
        
        self.pending_requests: deque = deque()
        self.batch_task: Optional[asyncio.Task] = None
        self.stats = {
            "total_requests": 0,
            "total_batches": 0,
            "avg_batch_size": 0,
            "avg_latency_ms": 0
        }
    
    async def predict(self, request: PredictionRequest) -> PredictionResult:
        """Add request to dynamic batch."""
        future = asyncio.Future()
        
        # Add to pending queue
        self.pending_requests.append((request, future, time.time()))
        self.stats["total_requests"] += 1
        
        # Start batch processing if not already running
        if not self.batch_task or self.batch_task.done():
            self.batch_task = asyncio.create_task(self._process_batches())
        
        return await future
    
    async def _process_batches(self) -> None:
        """Process requests in dynamic batches."""
        while self.pending_requests:
            batch_start_time = time.time()
            current_batch = []
            batch_futures = []
            
            # Collect batch based on size and time constraints
            while (len(current_batch) < self.max_batch_size and 
                   self.pending_requests):
                
                request, future, request_time = self.pending_requests.popleft()
                current_batch.append(request)
                batch_futures.append(future)
                
                # Check if we should process batch early
                oldest_request_age = (batch_start_time - request_time) * 1000
                if oldest_request_age > self.max_wait_time_ms:
                    break
            
            if not current_batch:
                break
            
            # Adjust batch size based on recent performance
            optimal_batch_size = self._calculate_optimal_batch_size()
            if len(current_batch) > optimal_batch_size:
                # Split large batch
                while len(current_batch) > optimal_batch_size:
                    sub_batch = current_batch[:optimal_batch_size]
                    sub_futures = batch_futures[:optimal_batch_size]
                    
                    current_batch = current_batch[optimal_batch_size:]
                    batch_futures = batch_futures[optimal_batch_size:]
                    
                    asyncio.create_task(
                        self._execute_batch(sub_batch, sub_futures)
                    )
            
            # Process remaining batch
            if current_batch:
                await self._execute_batch(current_batch, batch_futures)
            
            # Brief pause to allow new requests to accumulate
            if self.pending_requests:
                await asyncio.sleep(0.001)  # 1ms
    
    async def _execute_batch(
        self, 
        batch: list[PredictionRequest], 
        futures: list[asyncio.Future]
    ) -> None:
        """Execute batch inference."""
        try:
            start_time = time.time()
            
            # Perform batch inference
            results = await self._batch_predict(batch)
            
            end_time = time.time()
            batch_latency_ms = (end_time - start_time) * 1000
            
            # Update statistics
            self.stats["total_batches"] += 1
            self.stats["avg_batch_size"] = (
                (self.stats["avg_batch_size"] * (self.stats["total_batches"] - 1) + 
                 len(batch)) / self.stats["total_batches"]
            )
            self.stats["avg_latency_ms"] = (
                (self.stats["avg_latency_ms"] * (self.stats["total_batches"] - 1) + 
                 batch_latency_ms) / self.stats["total_batches"]
            )
            
            # Resolve futures
            for future, result in zip(futures, results):
                future.set_result(result)
                
        except Exception as e:
            # Resolve all futures with error
            for future in futures:
                if not future.done():
                    future.set_exception(e)
    
    def _calculate_optimal_batch_size(self) -> int:
        """Calculate optimal batch size based on performance."""
        if self.stats["total_batches"] < 10:
            return self.max_batch_size
        
        avg_latency = self.stats["avg_latency_ms"]
        
        # If we're above target latency, reduce batch size
        if avg_latency > self.target_latency_ms:
            return max(1, int(self.max_batch_size * 0.8))
        
        # If we're well below target, we can increase batch size
        elif avg_latency < self.target_latency_ms * 0.5:
            return min(self.max_batch_size * 2, 64)
        
        return self.max_batch_size
```

## WebSocket Performance

### Connection Management

#### Connection Pool Optimization

```python
import weakref
from collections import defaultdict

class OptimizedWebSocketServer:
    def __init__(self, config: WebSocketConfig):
        super().__init__(config)
        
        # Connection pools by origin for load balancing
        self.connection_pools = defaultdict(list)
        
        # Subscription indexing for fast lookup
        self.subscription_index = defaultdict(set)  # topic -> client_ids
        
        # Message queues for batching
        self.message_queues = defaultdict(deque)
        self.queue_flush_task = None
        
        # Connection statistics
        self.connection_stats = {
            "total_connections": 0,
            "active_connections": 0,
            "messages_per_second": 0,
            "avg_message_size": 0
        }
    
    async def add_client(self, client: WebSocketClient) -> None:
        """Add client with optimized indexing."""
        await super().add_client(client)
        
        # Add to connection pool by origin
        origin = client.origin or "unknown"
        self.connection_pools[origin].append(client.id)
        
        # Start message queue processing if needed
        if not self.queue_flush_task or self.queue_flush_task.done():
            self.queue_flush_task = asyncio.create_task(self._flush_message_queues())
    
    async def add_subscription(
        self, 
        client_id: str, 
        subscription: Subscription
    ) -> None:
        """Add subscription with indexing."""
        await super().add_subscription(client_id, subscription)
        
        # Index subscription for fast broadcast lookup
        subscription_key = f"{subscription.type.value}"
        self.subscription_index[subscription_key].add(client_id)
    
    async def broadcast_to_subscribers(
        self, 
        subscription_type: SubscriptionType,
        message_data: dict,
        filters: dict = None
    ) -> int:
        """Optimized broadcast using indexing."""
        
        subscription_key = subscription_type.value
        subscriber_ids = self.subscription_index.get(subscription_key, set())
        
        if not subscriber_ids:
            return 0
        
        # Batch messages by client for efficiency
        batched_messages = defaultdict(list)
        
        for client_id in subscriber_ids:
            client = self.clients.get(client_id)
            if not client or not client.authenticated:
                continue
            
            # Check subscription filters
            matching_subs = client.get_matching_subscriptions(
                subscription_type, filters
            )
            
            for subscription in matching_subs:
                message = WebSocketMessage(
                    type=MessageType.DATA,
                    data=message_data,
                    subscription_id=subscription.id
                )
                batched_messages[client_id].append(message)
        
        # Send batched messages
        sent_count = 0
        send_tasks = []
        
        for client_id, messages in batched_messages.items():
            client = self.clients.get(client_id)
            if client:
                task = self._send_batch_messages(client, messages)
                send_tasks.append(task)
        
        if send_tasks:
            results = await asyncio.gather(*send_tasks, return_exceptions=True)
            sent_count = sum(r for r in results if isinstance(r, int))
        
        return sent_count
    
    async def _send_batch_messages(
        self, 
        client: WebSocketClient, 
        messages: list[WebSocketMessage]
    ) -> int:
        """Send multiple messages efficiently."""
        if not messages:
            return 0
        
        try:
            # Option 1: Send as single batch message
            if len(messages) > 1 and self.config.enable_message_batching:
                batch_message = WebSocketMessage(
                    type=MessageType.DATA,
                    data={
                        "batch": True,
                        "messages": [msg.to_json() for msg in messages]
                    }
                )
                success = await client.send_message(batch_message)
                return len(messages) if success else 0
            
            # Option 2: Send individual messages concurrently
            else:
                send_tasks = [client.send_message(msg) for msg in messages]
                results = await asyncio.gather(*send_tasks, return_exceptions=True)
                return sum(1 for r in results if r is True)
                
        except Exception as e:
            logger.error("Batch message send failed", 
                        client_id=client.id, error=str(e))
            return 0
    
    async def _flush_message_queues(self) -> None:
        """Flush accumulated message queues periodically."""
        while self.is_running:
            try:
                # Process all queued messages
                for client_id, queue in self.message_queues.items():
                    if queue and client_id in self.clients:
                        client = self.clients[client_id]
                        messages = []
                        
                        # Drain queue
                        while queue and len(messages) < 50:  # Max 50 per batch
                            messages.append(queue.popleft())
                        
                        if messages:
                            await self._send_batch_messages(client, messages)
                
                # Small delay for queue accumulation
                await asyncio.sleep(0.01)  # 10ms
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Message queue flush error", error=str(e))
                await asyncio.sleep(0.1)
```

### Message Compression

#### Adaptive Compression

```python
class AdaptiveMessageCompression:
    def __init__(self):
        self.compression_stats = defaultdict(lambda: {
            "original_size": 0,
            "compressed_size": 0,
            "compression_time_ms": 0,
            "count": 0
        })
        self.compression_threshold = 1024  # 1KB
    
    def should_compress(self, message: str, message_type: str) -> bool:
        """Decide whether to compress based on size and type."""
        message_size = len(message.encode('utf-8'))
        
        # Don't compress small messages
        if message_size < self.compression_threshold:
            return False
        
        # Check compression effectiveness for this message type
        stats = self.compression_stats[message_type]
        if stats["count"] > 10:
            compression_ratio = stats["compressed_size"] / stats["original_size"]
            # Don't compress if compression ratio is poor (> 90%)
            if compression_ratio > 0.9:
                return False
        
        return True
    
    def compress_message(
        self, 
        message: str, 
        message_type: str
    ) -> tuple[bytes, dict]:
        """Compress message and update statistics."""
        import gzip
        import time
        
        start_time = time.time()
        original_size = len(message.encode('utf-8'))
        
        if not self.should_compress(message, message_type):
            return message.encode('utf-8'), {"compressed": False}
        
        # Compress message
        compressed_data = gzip.compress(message.encode('utf-8'))
        compressed_size = len(compressed_data)
        compression_time_ms = (time.time() - start_time) * 1000
        
        # Update statistics
        stats = self.compression_stats[message_type]
        stats["original_size"] += original_size
        stats["compressed_size"] += compressed_size
        stats["compression_time_ms"] += compression_time_ms
        stats["count"] += 1
        
        compression_ratio = compressed_size / original_size
        
        return compressed_data, {
            "compressed": True,
            "original_size": original_size,
            "compressed_size": compressed_size,
            "compression_ratio": compression_ratio,
            "compression_time_ms": compression_time_ms
        }
```

## Database Performance

### Connection Pooling

#### Optimized Database Pool

```python
from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine
import asyncio

class OptimizedDBConnectionPool:
    def __init__(self, database_url: str):
        # Optimized connection pool settings
        self.engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=20,              # Base connections
            max_overflow=30,           # Additional connections
            pool_timeout=30,           # Wait timeout
            pool_recycle=3600,         # Recycle after 1 hour
            pool_pre_ping=True,        # Validate connections
            
            # Connection optimization
            connect_args={
                "server_side_cursors": True,
                "application_name": "streaming_analytics",
                "tcp_keepalives_idle": "600",      # 10 minutes
                "tcp_keepalives_interval": "30",    # 30 seconds  
                "tcp_keepalives_count": "3",        # 3 retries
            }
        )
    
    async def execute_batch_insert(
        self, 
        table_name: str, 
        records: list[dict],
        batch_size: int = 1000
    ) -> None:
        """Optimized batch insert."""
        
        if not records:
            return
        
        # Process in batches to avoid memory issues
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Use COPY for PostgreSQL (fastest bulk insert)
            if self.engine.dialect.name == 'postgresql':
                await self._postgres_bulk_copy(table_name, batch)
            else:
                await self._generic_batch_insert(table_name, batch)
    
    async def _postgres_bulk_copy(
        self, 
        table_name: str, 
        records: list[dict]
    ) -> None:
        """Use PostgreSQL COPY for bulk insert."""
        import io
        import csv
        from sqlalchemy import text
        
        if not records:
            return
        
        # Convert records to CSV format
        csv_buffer = io.StringIO()
        fieldnames = records[0].keys()
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        
        for record in records:
            writer.writerow(record)
        
        csv_data = csv_buffer.getvalue()
        csv_buffer.close()
        
        # Execute COPY command
        copy_sql = f"""
        COPY {table_name} ({','.join(fieldnames)})
        FROM STDIN WITH CSV
        """
        
        async with self.engine.begin() as conn:
            # Use low-level connection for COPY
            raw_conn = await conn.get_raw_connection()
            cursor = raw_conn.cursor()
            
            cursor.copy_expert(copy_sql, io.StringIO(csv_data))
            await conn.commit()
```

### Query Optimization

#### Efficient Event Queries

```python
class OptimizedEventQueries:
    def __init__(self, db_session):
        self.db = db_session
    
    async def get_recent_events_batch(
        self,
        event_types: list[str],
        user_ids: list[str] = None,
        limit: int = 10000,
        time_window_hours: int = 24
    ) -> list[dict]:
        """Optimized query for recent events."""
        
        from sqlalchemy import text
        import datetime
        
        # Use parameterized query with proper indexing
        query = text("""
        SELECT 
            event_id,
            event_type,
            user_id,
            timestamp,
            payload
        FROM events 
        WHERE 
            timestamp >= :start_time
            AND event_type = ANY(:event_types)
            {user_filter}
        ORDER BY timestamp DESC
        LIMIT :limit
        """.format(
            user_filter="AND user_id = ANY(:user_ids)" if user_ids else ""
        ))
        
        params = {
            "start_time": datetime.datetime.utcnow() - datetime.timedelta(hours=time_window_hours),
            "event_types": event_types,
            "limit": limit
        }
        
        if user_ids:
            params["user_ids"] = user_ids
        
        result = await self.db.execute(query, params)
        return [dict(row) for row in result.fetchall()]
    
    async def get_aggregated_metrics(
        self,
        metric_name: str,
        group_by: str,
        time_bucket_minutes: int = 5,
        hours_back: int = 24
    ) -> list[dict]:
        """Optimized aggregation query using time buckets."""
        
        query = text("""
        SELECT 
            date_trunc('minute', timestamp) - 
            (EXTRACT(minute FROM timestamp)::int % :bucket_minutes) * interval '1 minute' as time_bucket,
            {group_by} as group_key,
            COUNT(*) as event_count,
            AVG((payload->>:metric_name)::float) as avg_value,
            MIN((payload->>:metric_name)::float) as min_value,
            MAX((payload->>:metric_name)::float) as max_value,
            STDDEV((payload->>:metric_name)::float) as stddev_value
        FROM events
        WHERE 
            timestamp >= :start_time
            AND payload ? :metric_name
            AND (payload->>:metric_name) ~ '^[0-9]*\.?[0-9]+$'
        GROUP BY time_bucket, {group_by}
        ORDER BY time_bucket DESC, group_key
        """.format(group_by=group_by))
        
        params = {
            "metric_name": metric_name,
            "bucket_minutes": time_bucket_minutes,
            "start_time": datetime.datetime.utcnow() - datetime.timedelta(hours=hours_back)
        }
        
        result = await self.db.execute(query, params)
        return [dict(row) for row in result.fetchall()]
```

## Memory Management

### Python Memory Optimization

#### Memory-Efficient Data Structures

```python
import sys
from typing import NamedTuple
from dataclasses import dataclass
import weakref

# Use slots to reduce memory overhead
@dataclass
class OptimizedEvent:
    __slots__ = ['event_id', 'event_type', 'user_id', 'timestamp', 'payload']
    
    event_id: str
    event_type: str
    user_id: str
    timestamp: float
    payload: dict

# Use NamedTuple for immutable data
class MetricPoint(NamedTuple):
    timestamp: float
    value: float
    tags: tuple  # Use tuple instead of dict for immutability

# Memory pool for frequent allocations
class EventPool:
    def __init__(self, pool_size: int = 10000):
        self.pool = []
        self.pool_size = pool_size
        
        # Pre-allocate events
        for _ in range(pool_size):
            self.pool.append(OptimizedEvent('', '', '', 0.0, {}))
    
    def get_event(self) -> OptimizedEvent:
        """Get event from pool or create new one."""
        if self.pool:
            return self.pool.pop()
        else:
            return OptimizedEvent('', '', '', 0.0, {})
    
    def return_event(self, event: OptimizedEvent) -> None:
        """Return event to pool after use."""
        if len(self.pool) < self.pool_size:
            # Reset event data
            event.event_id = ''
            event.event_type = ''
            event.user_id = ''
            event.timestamp = 0.0
            event.payload.clear()
            
            self.pool.append(event)

# Weak reference caches to prevent memory leaks
class WeakReferenceCache:
    def __init__(self, max_size: int = 1000):
        self.cache = weakref.WeakValueDictionary()
        self.max_size = max_size
        self.access_order = []
    
    def get(self, key: str) -> Any:
        """Get value from weak reference cache."""
        if key in self.cache:
            # Move to end of access order
            if key in self.access_order:
                self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]
        return None
    
    def put(self, key: str, value: Any) -> None:
        """Put value in weak reference cache."""
        # Clean up if cache is full
        while len(self.cache) >= self.max_size and self.access_order:
            oldest_key = self.access_order.pop(0)
            self.cache.pop(oldest_key, None)
        
        self.cache[key] = value
        self.access_order.append(key)
```

### Garbage Collection Tuning

```python
import gc
import threading
import time

class OptimizedGarbageCollection:
    def __init__(self):
        # Tune GC thresholds for streaming workload
        # Default is (700, 10, 10) - increase for better performance
        gc.set_threshold(2000, 20, 20)
        
        # Disable automatic GC and run manually
        gc.disable()
        
        # Start background GC thread
        self.gc_thread = threading.Thread(target=self._background_gc, daemon=True)
        self.gc_thread.start()
        
        self.gc_stats = {
            "collections": 0,
            "total_time_ms": 0,
            "avg_time_ms": 0
        }
    
    def _background_gc(self) -> None:
        """Run garbage collection in background thread."""
        while True:
            start_time = time.time()
            
            # Run GC manually during low-activity periods
            collected = gc.collect()
            
            gc_time_ms = (time.time() - start_time) * 1000
            
            # Update statistics
            self.gc_stats["collections"] += 1
            self.gc_stats["total_time_ms"] += gc_time_ms
            self.gc_stats["avg_time_ms"] = (
                self.gc_stats["total_time_ms"] / self.gc_stats["collections"]
            )
            
            if collected > 0:
                logger.debug(f"GC collected {collected} objects in {gc_time_ms:.2f}ms")
            
            # Sleep for 30 seconds between collections
            time.sleep(30)
    
    def force_gc(self) -> dict:
        """Force garbage collection and return stats."""
        start_time = time.time()
        collected = gc.collect()
        gc_time_ms = (time.time() - start_time) * 1000
        
        return {
            "objects_collected": collected,
            "gc_time_ms": gc_time_ms,
            "memory_usage_mb": self.get_memory_usage()
        }
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
```

## Monitoring Performance

### Performance Metrics Collection

```python
import time
import asyncio
from collections import defaultdict, deque
import statistics

class PerformanceMonitor:
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.metrics = defaultdict(lambda: deque(maxlen=window_size))
        self.counters = defaultdict(int)
        self.start_time = time.time()
    
    def record_latency(self, operation: str, latency_ms: float) -> None:
        """Record operation latency."""
        self.metrics[f"{operation}_latency_ms"].append(latency_ms)
        self.counters[f"{operation}_count"] += 1
    
    def record_throughput(self, operation: str, count: int = 1) -> None:
        """Record throughput metric."""
        current_time = time.time()
        self.metrics[f"{operation}_throughput"].append((current_time, count))
    
    def get_performance_stats(self) -> dict:
        """Get comprehensive performance statistics."""
        stats = {}
        current_time = time.time()
        
        for metric_name, values in self.metrics.items():
            if not values:
                continue
            
            if "latency_ms" in metric_name:
                # Latency statistics
                stats[metric_name] = {
                    "min": min(values),
                    "max": max(values),
                    "mean": statistics.mean(values),
                    "median": statistics.median(values),
                    "p95": self._percentile(values, 95),
                    "p99": self._percentile(values, 99),
                    "stddev": statistics.stdev(values) if len(values) > 1 else 0
                }
            
            elif "throughput" in metric_name:
                # Throughput statistics (events per second)
                recent_events = [
                    (ts, count) for ts, count in values
                    if current_time - ts <= 60  # Last minute
                ]
                
                if recent_events:
                    total_events = sum(count for _, count in recent_events)
                    time_span = max(1, current_time - min(ts for ts, _ in recent_events))
                    
                    stats[metric_name] = {
                        "events_per_second": total_events / time_span,
                        "total_events_last_minute": total_events,
                        "sample_count": len(recent_events)
                    }
        
        # Add counter statistics
        uptime_seconds = current_time - self.start_time
        for counter_name, count in self.counters.items():
            stats[counter_name] = {
                "total": count,
                "per_second": count / uptime_seconds if uptime_seconds > 0 else 0
            }
        
        return stats
    
    def _percentile(self, values: list, percentile: float) -> float:
        """Calculate percentile value."""
        if not values:
            return 0.0
        
        sorted_values = sorted(values)
        index = int((percentile / 100) * len(sorted_values))
        return sorted_values[min(index, len(sorted_values) - 1)]
```

## Best Practices

### Code Optimization
- Use appropriate data structures (deque, set, OrderedDict)
- Implement object pooling for frequent allocations
- Use asyncio for I/O-bound operations
- Use multiprocessing for CPU-bound operations

### Configuration Tuning
- Tune Kafka producer/consumer settings for workload
- Optimize database connection pooling
- Configure appropriate timeouts and buffer sizes
- Use compression where beneficial

### Monitoring and Alerting
- Monitor key performance metrics continuously
- Set up alerts for performance degradation
- Regular performance testing and baseline updates
- Capacity planning based on growth projections

### Resource Management
- Implement proper connection pooling
- Use memory-efficient data structures
- Regular garbage collection tuning
- Monitor and optimize memory usage