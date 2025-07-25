"""Real-time machine learning inference pipeline for streaming analytics."""

import asyncio
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import numpy as np
import structlog
from mlflow.entities.model_registry import ModelVersion

from libs.ml_models.registry import ModelRegistry

from .config import RealtimeMLConfig, get_streaming_config
from .event_store import EventSchema, EventType, MLPredictionEvent

logger = structlog.get_logger(__name__)


class PredictionStatus(str, Enum):
    """Status of ML predictions."""

    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"
    MODEL_NOT_FOUND = "model_not_found"
    FEATURE_ERROR = "feature_error"


@dataclass
class FeatureVector:
    """Feature vector for ML inference."""

    features: dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    entity_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_array(self, feature_names: list[str]) -> np.ndarray:
        """Convert to numpy array with specified feature order."""
        return np.array([self.features.get(name, 0.0) for name in feature_names])


@dataclass
class PredictionRequest:
    """Request for ML prediction."""

    model_name: str
    model_version: str | None = None
    features: FeatureVector | dict[str, Any] = None
    event: EventSchema | None = None
    request_id: str = field(
        default_factory=lambda: f"pred_{int(time.time() * 1000000)}"
    )
    timeout_ms: int = 5000
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PredictionResult:
    """Result of ML prediction."""

    request_id: str
    model_name: str
    model_version: str
    prediction: Any
    confidence_score: float | None = None
    feature_names: list[str] = field(default_factory=list)
    inference_time_ms: float = 0.0
    total_time_ms: float = 0.0
    status: PredictionStatus = PredictionStatus.SUCCESS
    error_message: str | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_event(self, source_service: str = "realtime_ml") -> MLPredictionEvent:
        """Convert prediction result to ML prediction event."""
        payload = {
            "request_id": self.request_id,
            "prediction": self.prediction,
            "inference_time_ms": self.inference_time_ms,
            "total_time_ms": self.total_time_ms,
            "status": self.status.value,
            "feature_count": len(self.feature_names),
        }

        if self.error_message:
            payload["error_message"] = self.error_message

        return MLPredictionEvent(
            event_name="ml_prediction_result",
            model_name=self.model_name,
            model_version=self.model_version,
            prediction_id=self.request_id,
            confidence_score=self.confidence_score,
            features_used=self.feature_names,
            source_service=source_service,
            payload=payload,
        )


class FeatureExtractor(ABC):
    """Abstract base class for feature extraction from events."""

    @abstractmethod
    def extract_features(self, event: EventSchema) -> FeatureVector | None:
        """Extract features from an event."""
        pass

    @abstractmethod
    def get_feature_names(self) -> list[str]:
        """Get list of feature names this extractor produces."""
        pass


class DefaultFeatureExtractor(FeatureExtractor):
    """Default feature extractor that uses event payload directly."""

    def __init__(self, feature_mapping: dict[str, str] | None = None):
        self.feature_mapping = feature_mapping or {}
        self.logger = logger.bind(component="default_feature_extractor")

    def extract_features(self, event: EventSchema) -> FeatureVector | None:
        """Extract features from event payload."""
        try:
            features = {}
            payload = event.payload

            # Apply feature mapping if provided
            if self.feature_mapping:
                for feature_name, payload_path in self.feature_mapping.items():
                    value = self._extract_nested_value(payload, payload_path)
                    if value is not None:
                        features[feature_name] = value
            else:
                # Use payload directly as features
                features = {
                    k: v
                    for k, v in payload.items()
                    if isinstance(v, int | float | str | bool)
                }

            # Add metadata features
            features.update(
                {
                    "event_type": event.event_type.value,
                    "source_service": event.source_service,
                    "timestamp_hour": event.timestamp.hour,
                    "timestamp_day_of_week": event.timestamp.weekday(),
                }
            )

            return FeatureVector(
                features=features,
                timestamp=event.timestamp,
                entity_id=event.event_id,
                metadata={"original_event_type": event.event_type.value},
            )

        except Exception as e:
            self.logger.error(
                "Feature extraction failed", event_id=event.event_id, error=str(e)
            )
            return None

    def get_feature_names(self) -> list[str]:
        """Get feature names (dynamic based on input)."""
        base_features = [
            "event_type",
            "source_service",
            "timestamp_hour",
            "timestamp_day_of_week",
        ]
        if self.feature_mapping:
            return list(self.feature_mapping.keys()) + base_features
        return base_features

    def _extract_nested_value(self, data: dict[str, Any], path: str) -> Any:
        """Extract nested value using dot notation path."""
        try:
            value = data
            for key in path.split("."):
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return None
            return value
        except Exception:
            return None


class ModelCache:
    """Cache for loaded ML models."""

    def __init__(self, max_size: int = 10):
        self.max_size = max_size
        self.cache: dict[str, tuple[Any, datetime]] = {}
        self.access_times: dict[str, datetime] = {}
        self.logger = logger.bind(component="model_cache")

    def get_model(self, model_key: str) -> Any | None:
        """Get model from cache."""
        if model_key in self.cache:
            model, cached_at = self.cache[model_key]
            self.access_times[model_key] = datetime.utcnow()
            self.logger.debug("Model cache hit", model_key=model_key)
            return model

        self.logger.debug("Model cache miss", model_key=model_key)
        return None

    def put_model(self, model_key: str, model: Any) -> None:
        """Put model in cache."""
        # Evict least recently used model if cache is full
        if len(self.cache) >= self.max_size:
            self._evict_lru()

        self.cache[model_key] = (model, datetime.utcnow())
        self.access_times[model_key] = datetime.utcnow()

        self.logger.info(
            "Model cached", model_key=model_key, cache_size=len(self.cache)
        )

    def remove_model(self, model_key: str) -> None:
        """Remove model from cache."""
        if model_key in self.cache:
            del self.cache[model_key]
            del self.access_times[model_key]
            self.logger.info("Model removed from cache", model_key=model_key)

    def _evict_lru(self) -> None:
        """Evict least recently used model."""
        if not self.access_times:
            return

        lru_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])

        self.remove_model(lru_key)
        self.logger.debug("Evicted LRU model", model_key=lru_key)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        return {
            "cache_size": len(self.cache),
            "max_size": self.max_size,
            "cached_models": list(self.cache.keys()),
        }


class RealtimeMLInferenceEngine:
    """Real-time ML inference engine."""

    def __init__(
        self,
        config: RealtimeMLConfig | None = None,
        model_registry: ModelRegistry | None = None,
    ):
        self.config = config or get_streaming_config().realtime_ml
        self.model_registry = model_registry or ModelRegistry()
        self.model_cache = ModelCache(self.config.model_cache_size)
        self.feature_extractors: dict[str, FeatureExtractor] = {}
        self.logger = logger.bind(component="realtime_ml_engine")

        # Performance tracking
        self._inference_count = 0
        self._error_count = 0
        self._total_inference_time_ms = 0.0

        # Batch processing
        self._batch_queue: list[PredictionRequest] = []
        self._batch_task: asyncio.Task | None = None
        self._is_running = False

    async def start(self) -> None:
        """Start the inference engine."""
        self._is_running = True

        # Start batch processing if enabled
        if self.config.batch_inference:
            self._batch_task = asyncio.create_task(self._process_batch_queue())

        self.logger.info(
            "Realtime ML inference engine started",
            batch_inference=self.config.batch_inference,
            max_batch_size=self.config.max_batch_size,
        )

    async def stop(self) -> None:
        """Stop the inference engine."""
        self._is_running = False

        # Stop batch processing
        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass

        # Process remaining batch queue
        if self._batch_queue:
            await self._process_batch(self._batch_queue.copy())
            self._batch_queue.clear()

        self.logger.info(
            "Realtime ML inference engine stopped",
            total_inferences=self._inference_count,
            total_errors=self._error_count,
        )

    def add_feature_extractor(
        self, event_type: EventType, extractor: FeatureExtractor
    ) -> None:
        """Add feature extractor for specific event type."""
        self.feature_extractors[event_type.value] = extractor
        self.logger.info(
            "Feature extractor added",
            event_type=event_type.value,
            extractor_type=type(extractor).__name__,
        )

    async def predict_from_event(
        self, event: EventSchema, model_name: str, model_version: str | None = None
    ) -> PredictionResult | None:
        """Make prediction from event data."""
        # Extract features from event
        extractor = self.feature_extractors.get(event.event_type.value)
        if not extractor:
            # Use default extractor
            extractor = DefaultFeatureExtractor()

        features = extractor.extract_features(event)
        if not features:
            return PredictionResult(
                request_id=f"event_{event.event_id}",
                model_name=model_name,
                model_version=model_version or "unknown",
                prediction=None,
                status=PredictionStatus.FEATURE_ERROR,
                error_message="Failed to extract features from event",
            )

        # Create prediction request
        request = PredictionRequest(
            model_name=model_name,
            model_version=model_version,
            features=features,
            event=event,
            request_id=f"event_{event.event_id}",
        )

        return await self.predict(request)

    async def predict(self, request: PredictionRequest) -> PredictionResult | None:
        """Make prediction from request."""
        start_time = time.time()

        try:
            # Handle batch vs single prediction
            if (
                self.config.batch_inference
                and len(self._batch_queue) < self.config.max_batch_size
            ):
                # Add to batch queue
                self._batch_queue.append(request)

                # Wait for batch to be processed
                timeout_seconds = request.timeout_ms / 1000
                deadline = time.time() + timeout_seconds

                while time.time() < deadline:
                    # Check if request was processed (removed from queue)
                    if request not in self._batch_queue:
                        break
                    await asyncio.sleep(0.01)  # 10ms polling

                # Request might have been processed in batch
                return None  # Results handled via batch callback

            else:
                # Single prediction
                return await self._single_predict(request)

        except Exception as e:
            total_time_ms = (time.time() - start_time) * 1000
            self.logger.error(
                "Prediction failed",
                request_id=request.request_id,
                model_name=request.model_name,
                error=str(e),
            )

            return PredictionResult(
                request_id=request.request_id,
                model_name=request.model_name,
                model_version=request.model_version or "unknown",
                prediction=None,
                total_time_ms=total_time_ms,
                status=PredictionStatus.ERROR,
                error_message=str(e),
            )

    async def _single_predict(self, request: PredictionRequest) -> PredictionResult:
        """Perform single prediction."""
        start_time = time.time()
        inference_start = None

        try:
            # Load model
            model_key = f"{request.model_name}:{request.model_version or 'latest'}"
            model = self.model_cache.get_model(model_key)

            if model is None:
                # Load from model registry
                model_version = await self._load_model(
                    request.model_name, request.model_version
                )
                if model_version is None:
                    return PredictionResult(
                        request_id=request.request_id,
                        model_name=request.model_name,
                        model_version=request.model_version or "unknown",
                        prediction=None,
                        total_time_ms=(time.time() - start_time) * 1000,
                        status=PredictionStatus.MODEL_NOT_FOUND,
                        error_message=f"Model {request.model_name} not found",
                    )

                model = model_version.model_artifact
                self.model_cache.put_model(model_key, model)

            # Prepare features
            if isinstance(request.features, FeatureVector):
                features = request.features
            else:
                features = FeatureVector(features=request.features or {})

            # Extract feature names from model or use defaults
            feature_names = getattr(
                model, "feature_names_", list(features.features.keys())
            )
            feature_array = features.to_array(feature_names)

            # Reshape for single prediction
            if feature_array.ndim == 1:
                feature_array = feature_array.reshape(1, -1)

            # Perform inference
            inference_start = time.time()

            if hasattr(model, "predict_proba"):
                # Classification with probabilities
                prediction_proba = model.predict_proba(feature_array)
                prediction = model.predict(feature_array)
                confidence_score = float(np.max(prediction_proba[0]))
                prediction_value = prediction[0] if len(prediction) > 0 else None
            elif hasattr(model, "predict"):
                # Standard prediction
                prediction = model.predict(feature_array)
                prediction_value = prediction[0] if len(prediction) > 0 else None
                confidence_score = None
            else:
                raise ValueError("Model does not support prediction")

            inference_time_ms = (time.time() - inference_start) * 1000
            total_time_ms = (time.time() - start_time) * 1000

            # Update statistics
            self._inference_count += 1
            self._total_inference_time_ms += inference_time_ms

            result = PredictionResult(
                request_id=request.request_id,
                model_name=request.model_name,
                model_version=request.model_version or "latest",
                prediction=prediction_value,
                confidence_score=confidence_score,
                feature_names=feature_names,
                inference_time_ms=inference_time_ms,
                total_time_ms=total_time_ms,
                status=PredictionStatus.SUCCESS,
            )

            self.logger.debug(
                "Prediction completed",
                request_id=request.request_id,
                model_name=request.model_name,
                inference_time_ms=inference_time_ms,
                total_time_ms=total_time_ms,
            )

            return result

        except Exception as e:
            self._error_count += 1
            inference_time_ms = (
                (time.time() - inference_start) * 1000 if inference_start else 0
            )
            total_time_ms = (time.time() - start_time) * 1000

            self.logger.error(
                "Single prediction failed",
                request_id=request.request_id,
                model_name=request.model_name,
                error=str(e),
            )

            return PredictionResult(
                request_id=request.request_id,
                model_name=request.model_name,
                model_version=request.model_version or "unknown",
                prediction=None,
                inference_time_ms=inference_time_ms,
                total_time_ms=total_time_ms,
                status=PredictionStatus.ERROR,
                error_message=str(e),
            )

    async def _load_model(
        self, model_name: str, model_version: str | None = None
    ) -> ModelVersion | None:
        """Load model from registry."""
        try:
            if model_version:
                return await self.model_registry.get_model(model_name, model_version)
            else:
                return await self.model_registry.get_latest_model_version(model_name)
        except Exception as e:
            self.logger.error(
                "Failed to load model",
                model_name=model_name,
                model_version=model_version,
                error=str(e),
            )
            return None

    async def _process_batch_queue(self) -> None:
        """Process batch queue periodically."""
        while self._is_running:
            try:
                if len(self._batch_queue) >= self.config.max_batch_size:
                    # Process full batch
                    batch = self._batch_queue[: self.config.max_batch_size]
                    self._batch_queue = self._batch_queue[self.config.max_batch_size :]
                    await self._process_batch(batch)

                elif self._batch_queue:
                    # Wait for batch timeout
                    await asyncio.sleep(self.config.batch_timeout_ms / 1000)

                    if self._batch_queue:
                        # Process partial batch
                        batch = self._batch_queue.copy()
                        self._batch_queue.clear()
                        await self._process_batch(batch)

                else:
                    # No items in queue, wait a bit
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in batch processing", error=str(e))
                await asyncio.sleep(1)  # Backoff on error

    async def _process_batch(
        self, batch: list[PredictionRequest]
    ) -> list[PredictionResult]:
        """Process a batch of prediction requests."""
        if not batch:
            return []

        self.logger.debug("Processing prediction batch", batch_size=len(batch))

        # Group by model for efficient batch processing
        model_batches: dict[str, list[PredictionRequest]] = {}

        for request in batch:
            model_key = f"{request.model_name}:{request.model_version or 'latest'}"
            if model_key not in model_batches:
                model_batches[model_key] = []
            model_batches[model_key].append(request)

        # Process each model batch
        all_results = []

        for model_key, requests in model_batches.items():
            try:
                model_results = await self._process_model_batch(model_key, requests)
                all_results.extend(model_results)
            except Exception as e:
                self.logger.error(
                    "Model batch processing failed",
                    model_key=model_key,
                    batch_size=len(requests),
                    error=str(e),
                )

                # Create error results
                for request in requests:
                    error_result = PredictionResult(
                        request_id=request.request_id,
                        model_name=request.model_name,
                        model_version=request.model_version or "unknown",
                        prediction=None,
                        status=PredictionStatus.ERROR,
                        error_message=str(e),
                    )
                    all_results.append(error_result)

        return all_results

    async def _process_model_batch(
        self, model_key: str, requests: list[PredictionRequest]
    ) -> list[PredictionResult]:
        """Process batch for a specific model."""
        start_time = time.time()
        results = []

        try:
            # Load model
            model = self.model_cache.get_model(model_key)
            if model is None:
                model_name, model_version = model_key.split(":", 1)
                model_version = model_version if model_version != "latest" else None

                model_version_obj = await self._load_model(model_name, model_version)
                if model_version_obj is None:
                    # Create error results for all requests
                    for request in requests:
                        results.append(
                            PredictionResult(
                                request_id=request.request_id,
                                model_name=request.model_name,
                                model_version=request.model_version or "unknown",
                                prediction=None,
                                status=PredictionStatus.MODEL_NOT_FOUND,
                                error_message=f"Model {model_key} not found",
                            )
                        )
                    return results

                model = model_version_obj.model_artifact
                self.model_cache.put_model(model_key, model)

            # Prepare batch features
            feature_vectors = []
            for request in requests:
                if isinstance(request.features, FeatureVector):
                    feature_vectors.append(request.features)
                else:
                    feature_vectors.append(
                        FeatureVector(features=request.features or {})
                    )

            # Get feature names and create batch array
            feature_names = getattr(
                model,
                "feature_names_",
                list(feature_vectors[0].features.keys()) if feature_vectors else [],
            )

            batch_features = np.array(
                [fv.to_array(feature_names) for fv in feature_vectors]
            )

            # Perform batch inference
            inference_start = time.time()

            if hasattr(model, "predict_proba"):
                predictions_proba = model.predict_proba(batch_features)
                predictions = model.predict(batch_features)
                confidence_scores = np.max(predictions_proba, axis=1)
            elif hasattr(model, "predict"):
                predictions = model.predict(batch_features)
                confidence_scores = [None] * len(predictions)
            else:
                raise ValueError("Model does not support prediction")

            inference_time_ms = (time.time() - inference_start) * 1000
            total_time_ms = (time.time() - start_time) * 1000

            # Create results
            for i, request in enumerate(requests):
                prediction_value = predictions[i] if i < len(predictions) else None
                confidence_score = (
                    confidence_scores[i] if confidence_scores[i] is not None else None
                )

                result = PredictionResult(
                    request_id=request.request_id,
                    model_name=request.model_name,
                    model_version=request.model_version or "latest",
                    prediction=prediction_value,
                    confidence_score=confidence_score,
                    feature_names=feature_names,
                    inference_time_ms=inference_time_ms
                    / len(requests),  # Amortized time
                    total_time_ms=total_time_ms / len(requests),
                    status=PredictionStatus.SUCCESS,
                )
                results.append(result)

            # Update statistics
            self._inference_count += len(requests)
            self._total_inference_time_ms += inference_time_ms

            self.logger.debug(
                "Batch inference completed",
                model_key=model_key,
                batch_size=len(requests),
                inference_time_ms=inference_time_ms,
                total_time_ms=total_time_ms,
            )

        except Exception as e:
            self._error_count += len(requests)
            self.logger.error(
                "Batch inference failed",
                model_key=model_key,
                batch_size=len(requests),
                error=str(e),
            )

            # Create error results
            for request in requests:
                results.append(
                    PredictionResult(
                        request_id=request.request_id,
                        model_name=request.model_name,
                        model_version=request.model_version or "unknown",
                        prediction=None,
                        status=PredictionStatus.ERROR,
                        error_message=str(e),
                    )
                )

        return results

    def get_stats(self) -> dict[str, Any]:
        """Get inference engine statistics."""
        avg_inference_time = (
            self._total_inference_time_ms / max(self._inference_count, 1)
            if self._inference_count > 0
            else 0.0
        )

        return {
            "is_running": self._is_running,
            "inference_count": self._inference_count,
            "error_count": self._error_count,
            "success_rate": self._inference_count
            / max(self._inference_count + self._error_count, 1),
            "average_inference_time_ms": avg_inference_time,
            "batch_queue_size": len(self._batch_queue),
            "model_cache": self.model_cache.get_stats(),
            "feature_extractors": list(self.feature_extractors.keys()),
        }


class RealtimeMLPipeline:
    """Complete real-time ML pipeline integrating inference with stream processing."""

    def __init__(
        self,
        config: RealtimeMLConfig | None = None,
        model_registry: ModelRegistry | None = None,
    ):
        self.config = config or get_streaming_config().realtime_ml
        self.inference_engine = RealtimeMLInferenceEngine(config, model_registry)
        self.logger = logger.bind(component="realtime_ml_pipeline")
        self._prediction_handlers: list[Callable[[PredictionResult], None]] = []

    async def start(self) -> None:
        """Start the ML pipeline."""
        await self.inference_engine.start()
        self.logger.info("Realtime ML pipeline started")

    async def stop(self) -> None:
        """Stop the ML pipeline."""
        await self.inference_engine.stop()
        self.logger.info("Realtime ML pipeline stopped")

    def add_prediction_handler(
        self, handler: Callable[[PredictionResult], None]
    ) -> None:
        """Add handler for prediction results."""
        self._prediction_handlers.append(handler)
        self.logger.info(
            "Prediction handler added", handler_count=len(self._prediction_handlers)
        )

    async def process_event_for_prediction(
        self, event: EventSchema, model_name: str, model_version: str | None = None
    ) -> None:
        """Process event and generate predictions."""
        try:
            # Make prediction
            result = await self.inference_engine.predict_from_event(
                event, model_name, model_version
            )

            if result:
                # Call prediction handlers
                await self._call_prediction_handlers(result)

                self.logger.debug(
                    "Event processed for prediction",
                    event_id=event.event_id,
                    model_name=model_name,
                    status=result.status,
                )

        except Exception as e:
            self.logger.error(
                "Error processing event for prediction",
                event_id=event.event_id,
                model_name=model_name,
                error=str(e),
            )

    async def _call_prediction_handlers(self, result: PredictionResult) -> None:
        """Call all registered prediction handlers."""
        for handler in self._prediction_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    result_coro = handler(result)
                    if result_coro is not None:
                        await result_coro
                else:
                    await asyncio.get_event_loop().run_in_executor(
                        None, handler, result
                    )
            except Exception as e:
                self.logger.error(
                    "Error in prediction handler",
                    handler=str(handler),
                    request_id=result.request_id,
                    error=str(e),
                )


# Global pipeline instance
_realtime_ml_pipeline: RealtimeMLPipeline | None = None


async def get_realtime_ml_pipeline() -> RealtimeMLPipeline:
    """Get the global realtime ML pipeline instance."""
    global _realtime_ml_pipeline

    if _realtime_ml_pipeline is None:
        _realtime_ml_pipeline = RealtimeMLPipeline()
        await _realtime_ml_pipeline.start()

    return _realtime_ml_pipeline


async def shutdown_realtime_ml_pipeline() -> None:
    """Shutdown the global realtime ML pipeline."""
    global _realtime_ml_pipeline

    if _realtime_ml_pipeline:
        await _realtime_ml_pipeline.stop()
        _realtime_ml_pipeline = None
