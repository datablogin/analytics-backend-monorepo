# ML Model Integration Guide

This guide covers integrating machine learning models with the streaming analytics infrastructure for real-time inference.

## Overview

The streaming analytics system provides a complete real-time ML pipeline featuring:

- **Real-time Inference**: Sub-50ms prediction latency
- **Model Registry Integration**: Automatic model loading from MLflow
- **Feature Store Integration**: Real-time feature retrieval
- **Batch Processing**: Efficient batch inference for high throughput
- **Model Caching**: In-memory model caching with automatic refresh
- **Performance monitoring**: Detailed inference metrics and alerting

## Architecture

```
Event Stream → Feature Extraction → Model Inference → Prediction Stream
     ↓              ↓                    ↓               ↓
   Kafka        Feature Store      Model Registry    WebSocket/Kafka
```

## Core Components

### RealtimeMLPipeline

The main class for real-time ML inference:

```python
from libs.streaming_analytics import RealtimeMLPipeline
from libs.streaming_analytics.config import get_streaming_config

config = get_streaming_config()
pipeline = RealtimeMLPipeline(config.realtime_ml)

# Initialize the pipeline
await pipeline.initialize()

# Make predictions
result = await pipeline.predict(
    model_name="user_churn_model",
    features={"age": 25, "usage_hours": 40, "last_login_days": 2}
)
```

### Configuration

ML pipeline configuration in `libs/streaming_analytics/config.py`:

```python
class RealtimeMLConfig(BaseModel):
    # Model settings
    model_cache_size: int = 10                    # Max models in cache
    model_warm_up_samples: int = 100              # Samples for model warm-up
    model_refresh_interval_seconds: int = 300     # Auto-refresh interval

    # Inference settings  
    batch_inference: bool = True                  # Enable batch processing
    max_batch_size: int = 32                      # Max batch size
    batch_timeout_ms: int = 100                   # Batch timeout

    # Feature store integration
    feature_cache_ttl_seconds: int = 300          # Feature cache TTL
    feature_freshness_threshold_ms: int = 60000   # Feature freshness limit

    # Performance targets
    max_latency_ms: int = 50                      # Target latency
    throughput_target_per_second: int = 1000      # Target throughput
```

### Environment Variables

```bash
# Model settings
STREAMING_REALTIME_ML__MODEL_CACHE_SIZE=20
STREAMING_REALTIME_ML__MODEL_REFRESH_INTERVAL_SECONDS=600

# Performance settings
STREAMING_REALTIME_ML__MAX_LATENCY_MS=30
STREAMING_REALTIME_ML__THROUGHPUT_TARGET_PER_SECOND=2000

# Batch processing
STREAMING_REALTIME_ML__BATCH_INFERENCE=true
STREAMING_REALTIME_ML__MAX_BATCH_SIZE=64
STREAMING_REALTIME_ML__BATCH_TIMEOUT_MS=50
```

## Model Registry Integration

### Model Registration

Register models in MLflow for streaming inference:

```python
import mlflow
from libs.ml_models.registry import ModelRegistry

# Register a model
registry = ModelRegistry()

model_info = await registry.register_model(
    name="user_churn_model",
    sklearn_model=trained_model,
    signature=model_signature,
    tags={
        "environment": "production",
        "use_case": "real_time_inference",
        "max_latency_ms": "50",
        "batch_size": "32"
    }
)

# Transition to production
await registry.transition_model_version_stage(
    name="user_churn_model",
    version=model_info.version,
    stage="Production"
)
```

### Model Loading

Models are automatically loaded from the registry:

```python
# Pipeline automatically loads the latest production model
pipeline = RealtimeMLPipeline()
await pipeline.initialize()

# Or specify a specific version
pipeline = RealtimeMLPipeline()
await pipeline.load_model(
    name="user_churn_model",
    version="3"  # Specific version
)
```

### Model Metadata

Include metadata for streaming optimization:

```python
model_metadata = {
    "inference_config": {
        "batch_size": 32,
        "max_latency_ms": 50,
        "preprocessing_steps": ["normalize", "encode_categorical"],
        "feature_names": ["age", "usage_hours", "last_login_days"],
        "output_type": "probability"
    },
    "deployment_config": {
        "min_replicas": 2,
        "max_replicas": 10,
        "cpu_request": "500m",
        "memory_request": "1Gi"
    }
}

await registry.log_model_metadata(
    name="user_churn_model",
    version="3",
    metadata=model_metadata
)
```

## Feature Engineering

### Real-time Feature Extraction

Extract features from streaming events:

```python
from libs.streaming_analytics.realtime_ml import FeatureExtractor

class UserChurnFeatureExtractor(FeatureExtractor):
    async def extract_features(self, event: EventSchema) -> FeatureVector:
        """Extract features from user action event."""
        user_id = event.payload.get("user_id")
        
        # Get features from feature store
        features = await self.get_user_features(user_id)
        
        # Add real-time features from event
        features.update({
            "current_session_duration": event.payload.get("session_duration", 0),
            "current_page_views": event.payload.get("page_views", 0),
            "time_since_last_action": self.calculate_time_since_last_action(event)
        })
        
        return FeatureVector(
            features=features,
            entity_id=user_id,
            timestamp=event.timestamp
        )
    
    async def get_user_features(self, user_id: str) -> dict:
        """Get user features from feature store."""
        # Implementation depends on your feature store
        # This is a simplified example
        return {
            "age": 25,
            "usage_hours_last_7d": 40,
            "last_login_days_ago": 2,
            "subscription_type": "premium",
            "num_purchases": 5
        }
```

### Feature Validation

Validate features before inference:

```python
from pydantic import BaseModel, validator

class ChurnPredictionFeatures(BaseModel):
    age: int
    usage_hours_last_7d: float
    last_login_days_ago: int
    subscription_type: str
    num_purchases: int
    
    @validator('age')
    def validate_age(cls, v):
        if not 13 <= v <= 120:
            raise ValueError('Age must be between 13 and 120')
        return v
    
    @validator('usage_hours_last_7d')
    def validate_usage_hours(cls, v):
        if v < 0:
            raise ValueError('Usage hours cannot be negative')
        return v

# Use in feature extractor
features_dict = await self.get_user_features(user_id)
validated_features = ChurnPredictionFeatures(**features_dict)
```

## Prediction Pipeline

### Basic Prediction Flow

```python
from libs.streaming_analytics import RealtimeMLPipeline, PredictionRequest

pipeline = RealtimeMLPipeline()

async def process_user_event(event: EventSchema):
    """Process user event and make churn prediction."""
    
    # Extract features
    feature_extractor = UserChurnFeatureExtractor()
    features = await feature_extractor.extract_features(event)
    
    # Create prediction request
    request = PredictionRequest(
        model_name="user_churn_model",
        features=features,
        event=event,
        timeout_ms=30
    )
    
    # Make prediction
    result = await pipeline.predict_single(request)
    
    # Handle result
    if result.status == PredictionStatus.SUCCESS:
        churn_probability = result.prediction[0]  # Assuming binary classifier
        
        if churn_probability > 0.8:
            await send_high_risk_alert(event.payload["user_id"], churn_probability)
        
        # Send prediction event
        prediction_event = result.to_event("churn_prediction_service")
        await publish_prediction_event(prediction_event)
    
    else:
        logger.error("Prediction failed", 
                    model=request.model_name,
                    error=result.error_message)
```

### Batch Prediction

For higher throughput, use batch processing:

```python
async def process_event_batch(events: list[EventSchema]):
    """Process a batch of events for efficient inference."""
    
    # Extract features for all events
    feature_requests = []
    for event in events:
        features = await feature_extractor.extract_features(event)
        request = PredictionRequest(
            model_name="user_churn_model",
            features=features,
            event=event
        )
        feature_requests.append(request)
    
    # Batch prediction
    results = await pipeline.predict_batch(feature_requests)
    
    # Process results
    for event, result in zip(events, results):
        if result.status == PredictionStatus.SUCCESS:
            await handle_prediction_result(event, result)
        else:
            await handle_prediction_error(event, result)
```

### Streaming Integration

Integrate with Kafka streams:

```python
from libs.streaming_analytics import StreamProcessor, KafkaManager

class MLPredictionProcessor(StreamProcessor):
    def __init__(self):
        super().__init__()
        self.ml_pipeline = RealtimeMLPipeline()
        self.kafka_manager = KafkaManager()
    
    async def initialize(self):
        await self.ml_pipeline.initialize()
        await self.kafka_manager.initialize()
    
    async def process_event(self, event: EventSchema) -> None:
        """Process event and make ML prediction."""
        
        # Skip non-relevant events
        if not self.should_predict(event):
            return
        
        # Extract features and predict
        features = await self.extract_features(event)
        request = PredictionRequest(
            model_name=self.get_model_name(event),
            features=features,
            event=event
        )
        
        result = await self.ml_pipeline.predict_single(request)
        
        # Publish prediction result
        if result.status == PredictionStatus.SUCCESS:
            await self.kafka_manager.produce_event(
                topic="ml-predictions",
                event=result.to_event()
            )

# Usage
processor = MLPredictionProcessor()
await processor.initialize()

# Process events from Kafka
await processor.consume_events(
    topic="analytics-events",
    consumer_group="ml-prediction-service"
)
```

## Model Performance Monitoring

### Metrics Collection

The pipeline automatically collects performance metrics:

```python
# Get pipeline statistics
stats = await pipeline.get_stats()

print(f"Total predictions: {stats['total_predictions']}")
print(f"Average latency: {stats['avg_latency_ms']:.2f}ms") 
print(f"Success rate: {stats['success_rate']:.2%}")
print(f"Models cached: {stats['models_cached']}")
```

### Custom Metrics

Add custom metrics for your use case:

```python
from libs.streaming_analytics.monitoring import StreamingMetrics

metrics = StreamingMetrics()

async def track_prediction_metrics(result: PredictionResult):
    """Track custom prediction metrics."""
    
    # Basic metrics
    metrics.increment_counter(
        "ml_predictions_total",
        tags={"model": result.model_name, "status": result.status.value}
    )
    
    metrics.record_histogram(
        "ml_inference_latency_ms",
        result.inference_time_ms,
        tags={"model": result.model_name}
    )
    
    # Business metrics
    if result.status == PredictionStatus.SUCCESS:
        prediction_value = result.prediction[0]
        
        metrics.record_histogram(
            "churn_probability_distribution",
            prediction_value,
            tags={"model": result.model_name}
        )
        
        # Alert on high-risk predictions
        if prediction_value > 0.8:
            metrics.increment_counter(
                "high_risk_churn_predictions",
                tags={"model": result.model_name}
            )
```

### Model Drift Detection

Monitor for model drift:

```python
from libs.streaming_analytics.monitoring import ModelDriftMonitor

class ChurnModelDriftMonitor(ModelDriftMonitor):
    def __init__(self):
        super().__init__()
        self.baseline_stats = self.load_baseline_stats()
    
    async def check_feature_drift(self, features: FeatureVector) -> dict:
        """Check for feature drift."""
        drift_scores = {}
        
        for feature_name, value in features.features.items():
            baseline_mean = self.baseline_stats[feature_name]["mean"]
            baseline_std = self.baseline_stats[feature_name]["std"]
            
            # Z-score drift detection
            z_score = abs((value - baseline_mean) / baseline_std)
            drift_scores[feature_name] = z_score
            
            # Alert on significant drift
            if z_score > 3.0:
                await self.send_drift_alert(feature_name, z_score, value)
        
        return drift_scores
    
    async def check_prediction_drift(self, predictions: list[float]) -> dict:
        """Check for prediction distribution drift."""
        current_mean = np.mean(predictions)
        baseline_mean = self.baseline_stats["predictions"]["mean"]
        
        # KS test for distribution drift
        from scipy.stats import ks_2samp
        statistic, p_value = ks_2samp(predictions, self.baseline_predictions)
        
        if p_value < 0.05:  # Significant drift
            await self.send_prediction_drift_alert(statistic, p_value)
        
        return {
            "ks_statistic": statistic,
            "p_value": p_value,
            "current_mean": current_mean,
            "baseline_mean": baseline_mean
        }
```

## Advanced Features

### A/B Testing Integration

Integrate with A/B testing framework:

```python
from libs.analytics_core.ab_testing import ABTestingFramework

class ABTestMLPipeline(RealtimeMLPipeline):
    def __init__(self, config: RealtimeMLConfig):
        super().__init__(config)
        self.ab_testing = ABTestingFramework()
    
    async def predict_with_ab_test(self, request: PredictionRequest) -> PredictionResult:
        """Make prediction with A/B test model selection."""
        
        user_id = request.event.payload.get("user_id")
        
        # Get A/B test assignment
        test_variant = await self.ab_testing.get_variant(
            experiment_key="churn_model_test",
            user_id=user_id
        )
        
        # Select model based on test variant
        if test_variant.variant_name == "new_model":
            model_name = "user_churn_model_v2"
        else:
            model_name = "user_churn_model_v1"
        
        # Update request with selected model
        request.model_name = model_name
        request.metadata["ab_test_variant"] = test_variant.variant_name
        
        # Make prediction
        result = await self.predict_single(request)
        
        # Track experiment metrics
        await self.ab_testing.track_conversion(
            experiment_key="churn_model_test",
            user_id=user_id,
            metric_name="prediction_confidence",
            value=result.confidence_score or 0.0
        )
        
        return result
```

### Multi-Model Ensemble

Use multiple models for ensemble predictions:

```python
class EnsembleMLPipeline(RealtimeMLPipeline):
    def __init__(self, config: RealtimeMLConfig):
        super().__init__(config)
        self.ensemble_models = [
            "churn_model_rf",      # Random Forest
            "churn_model_xgb",     # XGBoost  
            "churn_model_nn"       # Neural Network
        ]
        self.model_weights = [0.4, 0.4, 0.2]  # Weighted ensemble
    
    async def predict_ensemble(self, request: PredictionRequest) -> PredictionResult:
        """Make ensemble prediction using multiple models."""
        
        # Get predictions from all models
        predictions = []
        confidences = []
        
        for model_name in self.ensemble_models:
            model_request = PredictionRequest(
                model_name=model_name,
                features=request.features,
                event=request.event,
                timeout_ms=request.timeout_ms
            )
            
            result = await self.predict_single(model_request)
            
            if result.status == PredictionStatus.SUCCESS:
                predictions.append(result.prediction[0])
                confidences.append(result.confidence_score or 0.5)
            else:
                # Handle failed model gracefully
                predictions.append(0.5)  # Neutral prediction
                confidences.append(0.0)
        
        # Weighted ensemble
        ensemble_prediction = np.average(predictions, weights=self.model_weights)
        ensemble_confidence = np.average(confidences, weights=self.model_weights)
        
        return PredictionResult(
            request_id=request.request_id,
            model_name="ensemble_churn_model",
            model_version="1.0",
            prediction=[ensemble_prediction],
            confidence_score=ensemble_confidence,
            inference_time_ms=sum(result.inference_time_ms for result in results),
            status=PredictionStatus.SUCCESS,
            metadata={
                "individual_predictions": predictions,
                "individual_confidences": confidences,
                "model_weights": self.model_weights
            }
        )
```

## Testing and Validation

### Unit Testing

```python
import pytest
from libs.streaming_analytics import RealtimeMLPipeline, PredictionRequest, FeatureVector

@pytest.fixture
async def ml_pipeline():
    pipeline = RealtimeMLPipeline()
    await pipeline.initialize()
    return pipeline

@pytest.mark.asyncio
async def test_single_prediction(ml_pipeline):
    """Test single prediction."""
    features = FeatureVector(features={
        "age": 25,
        "usage_hours_last_7d": 40,
        "last_login_days_ago": 2
    })
    
    request = PredictionRequest(
        model_name="test_model",
        features=features,
        timeout_ms=1000
    )
    
    result = await ml_pipeline.predict_single(request)
    
    assert result.status == PredictionStatus.SUCCESS
    assert result.prediction is not None
    assert result.inference_time_ms > 0

@pytest.mark.asyncio
async def test_batch_prediction(ml_pipeline):
    """Test batch prediction."""
    requests = []
    for i in range(10):
        features = FeatureVector(features={
            "age": 20 + i,
            "usage_hours_last_7d": 30 + i,
            "last_login_days_ago": 1
        })
        
        request = PredictionRequest(
            model_name="test_model",
            features=features
        )
        requests.append(request)
    
    results = await ml_pipeline.predict_batch(requests)
    
    assert len(results) == 10
    assert all(r.status == PredictionStatus.SUCCESS for r in results)
```

### Integration Testing

```python
@pytest.mark.integration
async def test_ml_pipeline_integration():
    """Test full ML pipeline integration."""
    
    # Setup
    pipeline = RealtimeMLPipeline()
    kafka_manager = KafkaManager()
    
    await pipeline.initialize()
    await kafka_manager.initialize()
    
    # Create test event
    test_event = UserActionEvent(
        event_name="page_view",
        user_id="test_user_123",
        session_id="session_456",
        page_url="/dashboard",
        payload={"duration": 45}
    )
    
    # Process event through ML pipeline
    feature_extractor = UserChurnFeatureExtractor()
    features = await feature_extractor.extract_features(test_event)
    
    request = PredictionRequest(
        model_name="user_churn_model",
        features=features,
        event=test_event
    )
    
    result = await pipeline.predict_single(request)
    
    # Verify prediction
    assert result.status == PredictionStatus.SUCCESS
    assert 0.0 <= result.prediction[0] <= 1.0  # Probability
    assert result.inference_time_ms < 100      # Latency requirement
    
    # Verify event publishing
    prediction_event = result.to_event()
    await kafka_manager.produce_event("ml-predictions", prediction_event)
```

### Load Testing

```python
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

async def load_test_ml_pipeline():
    """Load test the ML pipeline."""
    
    pipeline = RealtimeMLPipeline()
    await pipeline.initialize()
    
    # Test parameters
    num_requests = 1000
    concurrent_requests = 50
    
    # Create test requests
    requests = []
    for i in range(num_requests):
        features = FeatureVector(features={
            "age": 20 + (i % 50),
            "usage_hours_last_7d": 30 + (i % 100),
            "last_login_days_ago": 1 + (i % 10)
        })
        
        request = PredictionRequest(
            model_name="user_churn_model",
            features=features
        )
        requests.append(request)
    
    # Execute load test
    start_time = time.time()
    
    # Process in batches
    results = []
    for i in range(0, num_requests, concurrent_requests):
        batch = requests[i:i + concurrent_requests]
        batch_results = await asyncio.gather(*[
            pipeline.predict_single(req) for req in batch
        ])
        results.extend(batch_results)
    
    end_time = time.time()
    
    # Analyze results
    total_time = end_time - start_time
    throughput = num_requests / total_time
    
    successful_predictions = sum(1 for r in results if r.status == PredictionStatus.SUCCESS)
    success_rate = successful_predictions / num_requests
    
    avg_latency = np.mean([r.inference_time_ms for r in results if r.status == PredictionStatus.SUCCESS])
    p95_latency = np.percentile([r.inference_time_ms for r in results if r.status == PredictionStatus.SUCCESS], 95)
    
    print(f"Load test results:")
    print(f"  Total time: {total_time:.2f}s")
    print(f"  Throughput: {throughput:.2f} req/s")
    print(f"  Success rate: {success_rate:.2%}")
    print(f"  Average latency: {avg_latency:.2f}ms")
    print(f"  P95 latency: {p95_latency:.2f}ms")
    
    # Assertions
    assert throughput >= 100, f"Throughput too low: {throughput}"
    assert success_rate >= 0.99, f"Success rate too low: {success_rate}"
    assert avg_latency <= 50, f"Average latency too high: {avg_latency}"
    assert p95_latency <= 100, f"P95 latency too high: {p95_latency}"
```

## Best Practices

### Model Deployment
- Use semantic versioning for models
- Test models in staging before production
- Implement gradual rollout for new models
- Monitor model performance continuously

### Feature Engineering
- Validate features before inference
- Cache frequently used features
- Handle missing features gracefully
- Log feature distributions for drift detection

### Performance Optimization
- Use batch inference when possible
- Implement model caching with TTL
- Monitor and optimize memory usage
- Use appropriate timeout values

### Error Handling
- Implement circuit breakers for model calls
- Use fallback models for critical paths
- Log all prediction errors with context
- Set up alerts for high error rates

### Monitoring
- Track prediction latency and throughput
- Monitor model accuracy over time
- Detect feature and prediction drift
- Set up business metric alerts