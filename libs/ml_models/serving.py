"""Model serving infrastructure for production ML inference."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import mlflow
import numpy as np
import pandas as pd
import structlog
from pydantic import BaseModel, Field

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig

logger = structlog.get_logger(__name__)


class ModelServingConfig(BaseConfig):
    """Configuration for model serving infrastructure."""

    # Model loading settings
    model_cache_size: int = Field(
        default=10, description="Maximum number of models to keep in memory"
    )
    model_cache_ttl_seconds: int = Field(
        default=3600, description="Time to live for cached models in seconds"
    )
    lazy_loading: bool = Field(default=True, description="Load models on first request")

    # Inference settings
    max_batch_size: int = Field(
        default=100, description="Maximum batch size for inference"
    )
    inference_timeout_seconds: float = Field(
        default=30.0, description="Timeout for inference requests"
    )
    prediction_log_sampling: float = Field(
        default=0.01, description="Fraction of predictions to log (0.0-1.0)"
    )

    # Health check settings
    health_check_interval_seconds: int = Field(
        default=60, description="Interval for model health checks"
    )
    max_consecutive_failures: int = Field(
        default=5, description="Max failures before marking model unhealthy"
    )

    # Performance settings
    enable_performance_monitoring: bool = Field(
        default=True, description="Enable performance monitoring"
    )
    enable_input_validation: bool = Field(
        default=True, description="Enable input validation"
    )
    enable_output_validation: bool = Field(
        default=True, description="Enable output validation"
    )

    # Auto-scaling settings
    enable_auto_scaling: bool = Field(
        default=False, description="Enable auto-scaling based on load"
    )
    scale_up_threshold: float = Field(
        default=0.8, description="CPU/memory threshold to scale up"
    )
    scale_down_threshold: float = Field(
        default=0.3, description="CPU/memory threshold to scale down"
    )

    # Caching strategy settings
    cache_strategy: str = Field(
        default="lru", description="Cache eviction strategy (lru, lfu, size_aware)"
    )
    large_model_threshold_mb: float = Field(
        default=500.0, description="Threshold for considering a model large (MB)"
    )
    small_model_cache_ttl_seconds: int = Field(
        default=7200, description="TTL for small models (2 hours)"
    )
    large_model_cache_ttl_seconds: int = Field(
        default=14400, description="TTL for large models (4 hours)"
    )
    high_usage_model_priority: bool = Field(
        default=True, description="Prioritize high-usage models in cache"
    )


class InferenceRequest(BaseModel):
    """Request for model inference."""

    model_name: str = Field(description="Name of the model")
    model_version: str | None = Field(
        default=None, description="Model version (uses latest if not specified)"
    )
    model_stage: str | None = Field(
        default=None, description="Model stage (Production, Staging, etc.)"
    )

    # Input data
    inputs: dict[str, Any] | list[dict[str, Any]] = Field(
        description="Input data for inference"
    )

    # Request metadata
    request_id: str | None = Field(
        default=None, description="Unique request ID for tracking"
    )
    return_probabilities: bool = Field(
        default=False, description="Return prediction probabilities"
    )
    return_explanations: bool = Field(
        default=False, description="Return model explanations"
    )

    # Configuration
    batch_size: int | None = Field(
        default=None, description="Batch size for processing"
    )
    timeout_seconds: float | None = Field(default=None, description="Request timeout")


class InferenceResponse(BaseModel):
    """Response from model inference."""

    # Results
    predictions: list[Any] = Field(description="Model predictions")
    probabilities: list[list[float]] | None = Field(
        default=None, description="Prediction probabilities"
    )
    explanations: list[dict[str, Any]] | None = Field(
        default=None, description="Model explanations"
    )

    # Metadata
    model_name: str = Field(description="Model name used")
    model_version: str = Field(description="Model version used")
    model_stage: str | None = Field(description="Model stage used")

    # Performance metrics
    inference_time_ms: float = Field(description="Inference time in milliseconds")
    preprocessing_time_ms: float = Field(description="Preprocessing time")
    postprocessing_time_ms: float = Field(description="Postprocessing time")

    # Request tracking
    request_id: str | None = Field(description="Request ID")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Status
    status: str = Field(default="success", description="Response status")
    error_message: str | None = Field(
        default=None, description="Error message if failed"
    )


class ModelContainer:
    """Container for a loaded model with metadata."""

    def __init__(
        self,
        name: str,
        version: str,
        stage: str,
        model: Any,
        signature: Any = None,
        input_example: Any = None,
        model_size_mb: float | None = None,
    ):
        self.name = name
        self.version = version
        self.stage = stage
        self.model = model
        self.signature = signature
        self.input_example = input_example

        # Performance tracking
        self.load_time = datetime.now(timezone.utc)
        self.last_used = datetime.now(timezone.utc)
        self.usage_count = 0
        self.failure_count = 0
        self.last_failure = None

        # Health status
        self.healthy = True
        self.consecutive_failures = 0

        # Model characteristics for caching strategy
        self.model_size_mb = model_size_mb or self._estimate_model_size()
        self.priority_score = 0.0  # Dynamic priority based on usage and size

    def update_usage(self) -> None:
        """Update usage statistics."""
        self.last_used = datetime.now(timezone.utc)
        self.usage_count += 1

    def record_failure(self, error: Exception) -> None:
        """Record inference failure."""
        self.failure_count += 1
        self.last_failure = datetime.now(timezone.utc)
        self.consecutive_failures += 1

        # Mark unhealthy if too many consecutive failures
        if self.consecutive_failures >= 5:
            self.healthy = False

    def record_success(self) -> None:
        """Record successful inference."""
        self.consecutive_failures = 0
        self.healthy = True

    @property
    def age_seconds(self) -> float:
        """Age of the model in seconds."""
        return (datetime.now(timezone.utc) - self.load_time).total_seconds()

    @property
    def idle_seconds(self) -> float:
        """Time since last use in seconds."""
        return (datetime.now(timezone.utc) - self.last_used).total_seconds()

    def _estimate_model_size(self) -> float:
        """Estimate model size in MB."""
        try:
            import sys
            import pickle

            # Try to get size using pickle serialization (rough estimate)
            try:
                model_bytes = pickle.dumps(self.model, protocol=pickle.HIGHEST_PROTOCOL)
                return len(model_bytes) / (1024 * 1024)  # Convert to MB
            except Exception:
                # Fallback to sys.getsizeof (less accurate but works)
                size_bytes = sys.getsizeof(self.model)
                return size_bytes / (1024 * 1024)  # Convert to MB

        except Exception:
            # Default size if estimation fails
            return 50.0  # 50 MB default

    def calculate_priority_score(self, config: "ModelServingConfig") -> float:
        """Calculate priority score for cache eviction."""
        try:
            # Base score from usage frequency
            usage_score = min(self.usage_count / 100, 1.0)  # Normalize to 0-1

            # Recency score (more recent = higher score)
            max_idle_time = 24 * 3600  # 24 hours
            recency_score = max(0, 1 - (self.idle_seconds / max_idle_time))

            # Size penalty (larger models get lower priority unless heavily used)
            size_penalty = 1.0
            if self.model_size_mb > config.large_model_threshold_mb:
                size_penalty = 0.7  # Large models get 30% penalty

            # High usage bonus
            usage_bonus = 1.0
            if config.high_usage_model_priority and self.usage_count > 50:
                usage_bonus = 1.3  # 30% bonus for high usage models

            # Health penalty
            health_multiplier = 1.0 if self.healthy else 0.5

            # Combined score
            priority_score = (
                (usage_score * 0.4 + recency_score * 0.6)
                * size_penalty
                * usage_bonus
                * health_multiplier
            )

            self.priority_score = priority_score
            return priority_score

        except Exception:
            # Fallback to simple recency-based score
            return max(0, 1 - (self.idle_seconds / (24 * 3600)))

    def get_cache_ttl(self, config: "ModelServingConfig") -> int:
        """Get cache TTL based on model characteristics."""
        if self.model_size_mb > config.large_model_threshold_mb:
            return config.large_model_cache_ttl_seconds
        else:
            return config.small_model_cache_ttl_seconds

    def should_evict(self, config: "ModelServingConfig") -> bool:
        """Determine if this model should be evicted based on TTL and characteristics."""
        ttl = self.get_cache_ttl(config)

        # Check basic TTL
        if self.age_seconds > ttl:
            return True

        # High-usage models get extended TTL
        if config.high_usage_model_priority and self.usage_count > 100:
            extended_ttl = ttl * 1.5  # 50% extension for high-usage models
            return self.age_seconds > extended_ttl

        return False


class ModelServer:
    """Production model serving infrastructure."""

    def __init__(self, config: ModelServingConfig | None = None):
        """Initialize model server."""
        self.config = config or ModelServingConfig()
        self.metrics = MLOpsMetrics()

        # Model cache
        self.model_cache: dict[str, ModelContainer] = {}
        self.cache_lock = asyncio.Lock()

        # Performance tracking
        self.request_count = 0
        self.error_count = 0
        self.start_time = datetime.now(timezone.utc)

        # Health check task
        self._health_check_task = None

        logger.info(
            "Model server initialized",
            cache_size=self.config.model_cache_size,
            lazy_loading=self.config.lazy_loading,
        )

    async def start(self) -> None:
        """Start the model server."""
        # Start health check task
        if self.config.health_check_interval_seconds > 0:
            self._health_check_task = asyncio.create_task(self._health_check_loop())

        logger.info("Model server started")

    async def stop(self) -> None:
        """Stop the model server."""
        # Cancel health check task
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # Clear model cache
        async with self.cache_lock:
            self.model_cache.clear()

        logger.info("Model server stopped")

    async def predict(self, request: InferenceRequest) -> InferenceResponse:
        """Make predictions using the specified model."""
        start_time = time.time()
        request_id = request.request_id or f"req_{int(time.time() * 1000)}"

        try:
            self.request_count += 1

            # Get model
            model_container = await self._get_model(
                name=request.model_name,
                version=request.model_version,
                stage=request.model_stage,
            )

            if not model_container.healthy:
                raise ValueError(f"Model {request.model_name} is unhealthy")

            # Preprocess inputs
            preprocess_start = time.time()
            processed_inputs = await self._preprocess_inputs(
                inputs=request.inputs,
                model_container=model_container,
            )
            preprocess_time = (time.time() - preprocess_start) * 1000

            # Validate inputs
            if self.config.enable_input_validation:
                await self._validate_inputs(processed_inputs, model_container)

            # Make predictions
            inference_start = time.time()
            raw_predictions = await self._run_inference(
                inputs=processed_inputs,
                model_container=model_container,
                return_probabilities=request.return_probabilities,
            )
            inference_time = (time.time() - inference_start) * 1000

            # Postprocess outputs
            postprocess_start = time.time()
            predictions, probabilities = await self._postprocess_outputs(
                raw_predictions=raw_predictions,
                model_container=model_container,
                return_probabilities=request.return_probabilities,
            )
            postprocess_time = (time.time() - postprocess_start) * 1000

            # Validate outputs
            if self.config.enable_output_validation:
                await self._validate_outputs(predictions, model_container)

            # Get explanations if requested
            explanations = None
            if request.return_explanations:
                explanations = await self._get_explanations(
                    inputs=processed_inputs,
                    predictions=predictions,
                    model_container=model_container,
                )

            # Update model usage
            model_container.update_usage()
            model_container.record_success()

            # Record metrics
            total_time = (time.time() - start_time) * 1000
            self.metrics.record_model_inference(
                model_name=request.model_name,
                model_version=model_container.version,
                inference_time_ms=inference_time,
                total_time_ms=total_time,
                batch_size=len(predictions),
                success=True,
            )

            # Log prediction sample
            if np.random.random() < self.config.prediction_log_sampling:
                logger.info(
                    "Prediction sample",
                    request_id=request_id,
                    model_name=request.model_name,
                    model_version=model_container.version,
                    input_sample=str(processed_inputs)[:100] + "...",
                    prediction_sample=str(predictions[:3]),
                    inference_time_ms=inference_time,
                )

            return InferenceResponse(
                predictions=predictions,
                probabilities=probabilities,
                explanations=explanations,
                model_name=request.model_name,
                model_version=model_container.version,
                model_stage=model_container.stage,
                inference_time_ms=inference_time,
                preprocessing_time_ms=preprocess_time,
                postprocessing_time_ms=postprocess_time,
                request_id=request_id,
            )

        except Exception as error:
            self.error_count += 1
            error_time = (time.time() - start_time) * 1000

            # Record error metrics
            self.metrics.record_model_inference(
                model_name=request.model_name,
                model_version="unknown",
                inference_time_ms=0,
                total_time_ms=error_time,
                batch_size=0,
                success=False,
                error_type=type(error).__name__,
            )

            # Update model failure count
            if request.model_name in self.model_cache:
                self.model_cache[request.model_name].record_failure(error)

            logger.error(
                "Inference failed",
                request_id=request_id,
                model_name=request.model_name,
                error=str(error),
                error_time_ms=error_time,
            )

            return InferenceResponse(
                predictions=[],
                model_name=request.model_name,
                model_version="unknown",
                model_stage=None,
                inference_time_ms=0,
                preprocessing_time_ms=0,
                postprocessing_time_ms=0,
                request_id=request_id,
                status="error",
                error_message=str(error),
            )

    async def _get_model(
        self,
        name: str,
        version: str | None = None,
        stage: str | None = None,
    ) -> ModelContainer:
        """Get model from cache or load it."""
        cache_key = f"{name}:{version or 'latest'}:{stage or 'none'}"

        async with self.cache_lock:
            # Check cache
            if cache_key in self.model_cache:
                model_container = self.model_cache[cache_key]

                # Check if model is still fresh
                if model_container.age_seconds < self.config.model_cache_ttl_seconds:
                    return model_container
                else:
                    # Remove expired model
                    del self.model_cache[cache_key]

            # Load model
            model_container = await self._load_model(name, version, stage)

            # Add to cache (with eviction if needed)
            if len(self.model_cache) >= self.config.model_cache_size:
                await self._evict_model_by_strategy()

            self.model_cache[cache_key] = model_container

            return model_container

    async def _load_model(
        self,
        name: str,
        version: str | None = None,
        stage: str | None = None,
    ) -> ModelContainer:
        """Load model from MLflow."""
        try:
            load_start = time.time()

            # Construct model URI
            if version:
                model_uri = f"models:/{name}/{version}"
                actual_version = version
                actual_stage = "None"
            elif stage:
                model_uri = f"models:/{name}/{stage}"
                # Get actual version from registry
                client = mlflow.MlflowClient()
                latest_versions = client.get_latest_versions(name, stages=[stage])
                if not latest_versions:
                    raise ValueError(f"No model found for {name} in stage {stage}")
                actual_version = latest_versions[0].version
                actual_stage = stage
            else:
                # Get latest version
                client = mlflow.MlflowClient()
                latest_versions = client.get_latest_versions(name)
                if not latest_versions:
                    raise ValueError(f"No model versions found for {name}")
                actual_version = latest_versions[0].version
                actual_stage = latest_versions[0].current_stage
                model_uri = f"models:/{name}/{actual_version}"

            # Load model
            model = mlflow.pyfunc.load_model(model_uri)

            # Try to get model signature and input example
            signature = None
            input_example = None
            try:
                model_info = mlflow.models.get_model_info(model_uri)
                signature = model_info.signature
                input_example = model_info.saved_input_example_info
            except Exception:
                logger.warning("Could not load model signature or input example")

            load_time = time.time() - load_start

            # Record metrics
            self.metrics.record_model_load(
                model_name=name,
                model_version=actual_version,
                load_time_seconds=load_time,
                success=True,
            )

            logger.info(
                "Model loaded",
                name=name,
                version=actual_version,
                stage=actual_stage,
                load_time_seconds=load_time,
            )

            return ModelContainer(
                name=name,
                version=actual_version,
                stage=actual_stage,
                model=model,
                signature=signature,
                input_example=input_example,
            )

        except Exception as error:
            # Record error metrics
            self.metrics.record_model_load(
                model_name=name,
                model_version=version or "unknown",
                load_time_seconds=0,
                success=False,
                error_type=type(error).__name__,
            )

            logger.error(
                "Failed to load model",
                name=name,
                version=version,
                stage=stage,
                error=str(error),
            )
            raise

    async def _evict_model_by_strategy(self) -> None:
        """Evict model from cache using configured strategy."""
        if not self.model_cache:
            return

        strategy = self.config.cache_strategy.lower()

        if strategy == "size_aware":
            eviction_key = self._select_model_for_eviction_size_aware()
        elif strategy == "lfu":  # Least Frequently Used
            eviction_key = self._select_model_for_eviction_lfu()
        else:  # Default to LRU (Least Recently Used)
            eviction_key = self._select_model_for_eviction_lru()

        if eviction_key:
            evicted_model = self.model_cache.pop(eviction_key)

            logger.info(
                "Model evicted from cache",
                name=evicted_model.name,
                version=evicted_model.version,
                age_seconds=evicted_model.age_seconds,
                usage_count=evicted_model.usage_count,
                model_size_mb=evicted_model.model_size_mb,
                strategy=strategy,
                priority_score=evicted_model.priority_score,
            )

    def _select_model_for_eviction_lru(self) -> str | None:
        """Select model for eviction using LRU strategy."""
        if not self.model_cache:
            return None

        return min(
            self.model_cache.keys(),
            key=lambda k: self.model_cache[k].last_used,
        )

    def _select_model_for_eviction_lfu(self) -> str | None:
        """Select model for eviction using LFU strategy."""
        if not self.model_cache:
            return None

        return min(
            self.model_cache.keys(),
            key=lambda k: self.model_cache[k].usage_count,
        )

    def _select_model_for_eviction_size_aware(self) -> str | None:
        """Select model for eviction using size-aware strategy."""
        if not self.model_cache:
            return None

        # Calculate priority scores for all models
        model_priorities = []
        for cache_key, model_container in self.model_cache.items():
            priority = model_container.calculate_priority_score(self.config)
            model_priorities.append((cache_key, priority))

        # Sort by priority (lowest first for eviction)
        model_priorities.sort(key=lambda x: x[1])

        # Prefer evicting larger, lower-priority models
        for cache_key, priority in model_priorities:
            model_container = self.model_cache[cache_key]

            # Check if model should be evicted based on characteristics
            if model_container.should_evict(self.config):
                return cache_key

        # If no model meets eviction criteria, evict lowest priority
        return model_priorities[0][0] if model_priorities else None

    # Maintain backward compatibility
    async def _evict_oldest_model(self) -> None:
        """Legacy method - delegate to strategy-based eviction."""
        await self._evict_model_by_strategy()

    async def _preprocess_inputs(
        self,
        inputs: dict[str, Any] | list[dict[str, Any]],
        model_container: ModelContainer,
    ) -> pd.DataFrame:
        """Preprocess inputs for inference with robust validation."""
        try:
            # Validate and convert inputs to DataFrame
            df = self._validate_and_convert_inputs(inputs)

            # Validate DataFrame structure
            if df.empty:
                raise ValueError("Input data resulted in empty DataFrame")

            # Check for any completely null rows
            null_rows = df.isnull().all(axis=1).sum()
            if null_rows > 0:
                logger.warning(f"Found {null_rows} completely null rows in input data")

            # Apply any model-specific preprocessing
            # This is a placeholder for custom preprocessing logic

            logger.debug(
                "Input preprocessing completed",
                input_shape=df.shape,
                columns=list(df.columns),
            )

            return df

        except Exception as error:
            logger.error("Failed to preprocess inputs", error=str(error))
            raise

    def _validate_and_convert_inputs(
        self, inputs: dict[str, Any] | list[dict[str, Any]]
    ) -> pd.DataFrame:
        """Validate input format and convert to DataFrame."""
        if isinstance(inputs, dict):
            # Single record case
            if not inputs:
                raise ValueError("Input dictionary cannot be empty")

            # Validate all values are serializable
            for key, value in inputs.items():
                if not isinstance(key, str):
                    raise ValueError(
                        f"All input keys must be strings, got {type(key)} for key: {key}"
                    )

                # Check for problematic values
                if value is not None and not isinstance(
                    value, (str, int, float, bool, list)
                ):
                    logger.warning(
                        f"Input value for key '{key}' may not be serializable: {type(value)}"
                    )

            return pd.DataFrame([inputs])

        elif isinstance(inputs, list):
            # Batch case
            if not inputs:
                raise ValueError("Input list cannot be empty")

            # Validate all items are dictionaries
            for i, item in enumerate(inputs):
                if not isinstance(item, dict):
                    raise ValueError(
                        f"List item at index {i} must be a dict, got {type(item)}"
                    )

                if not item:
                    raise ValueError(f"Input dictionary at index {i} cannot be empty")

            # Check for consistent keys across all dictionaries
            if len(inputs) > 1:
                first_keys = set(inputs[0].keys())
                for i, item in enumerate(inputs[1:], 1):
                    item_keys = set(item.keys())
                    if item_keys != first_keys:
                        missing_keys = first_keys - item_keys
                        extra_keys = item_keys - first_keys

                        warning_msg = f"Inconsistent keys in input at index {i}"
                        if missing_keys:
                            warning_msg += f". Missing: {missing_keys}"
                        if extra_keys:
                            warning_msg += f". Extra: {extra_keys}"

                        logger.warning(warning_msg)

            return pd.DataFrame(inputs)

        else:
            raise ValueError(
                f"Inputs must be dict or list of dicts, got {type(inputs)}. "
                f"Supported formats: dict for single inference, list[dict] for batch inference"
            )

    async def _validate_inputs(
        self,
        inputs: pd.DataFrame,
        model_container: ModelContainer,
    ) -> None:
        """Validate input data."""
        try:
            # Check batch size
            if len(inputs) > self.config.max_batch_size:
                raise ValueError(
                    f"Batch size {len(inputs)} exceeds maximum {self.config.max_batch_size}"
                )

            # Validate against model signature if available
            if model_container.signature and hasattr(
                model_container.signature, "inputs"
            ):
                # This is a simplified validation - in practice you'd want more robust checking
                expected_columns = set(model_container.signature.inputs.input_names())
                actual_columns = set(inputs.columns)

                if not expected_columns.issubset(actual_columns):
                    missing_columns = expected_columns - actual_columns
                    raise ValueError(f"Missing required columns: {missing_columns}")

        except Exception as error:
            logger.error("Input validation failed", error=str(error))
            raise

    async def _run_inference(
        self,
        inputs: pd.DataFrame,
        model_container: ModelContainer,
        return_probabilities: bool = False,
    ) -> Any:
        """Run model inference."""
        try:
            # Run prediction
            if hasattr(model_container.model, "predict"):
                predictions = model_container.model.predict(inputs)
            else:
                # Fallback to pyfunc interface
                predictions = model_container.model.predict(inputs)

            # Get probabilities if requested and model supports it
            probabilities = None
            if return_probabilities:
                if hasattr(model_container.model, "predict_proba"):
                    probabilities = model_container.model.predict_proba(inputs)
                elif hasattr(model_container.model, "predict") and hasattr(
                    predictions, "shape"
                ):
                    # For models that return probabilities directly
                    if len(predictions.shape) > 1 and predictions.shape[1] > 1:
                        probabilities = predictions

            return {
                "predictions": predictions,
                "probabilities": probabilities,
            }

        except Exception as error:
            logger.error("Inference failed", error=str(error))
            raise

    async def _postprocess_outputs(
        self,
        raw_predictions: dict[str, Any],
        model_container: ModelContainer,
        return_probabilities: bool = False,
    ) -> tuple[list[Any], list[list[float]] | None]:
        """Postprocess model outputs."""
        try:
            predictions = raw_predictions["predictions"]
            probabilities = raw_predictions.get("probabilities")

            # Convert to list format
            if hasattr(predictions, "tolist"):
                predictions = predictions.tolist()
            elif not isinstance(predictions, list):
                predictions = [predictions]

            if probabilities is not None and hasattr(probabilities, "tolist"):
                probabilities = probabilities.tolist()

            return predictions, probabilities

        except Exception as error:
            logger.error("Failed to postprocess outputs", error=str(error))
            raise

    async def _validate_outputs(
        self,
        predictions: list[Any],
        model_container: ModelContainer,
    ) -> None:
        """Validate output data."""
        try:
            # Basic validation
            if not predictions:
                raise ValueError("No predictions returned")

            # Check for NaN values
            if any(pd.isna(pred) for pred in predictions if pred is not None):
                raise ValueError("Predictions contain NaN values")

        except Exception as error:
            logger.error("Output validation failed", error=str(error))
            raise

    async def _get_explanations(
        self,
        inputs: pd.DataFrame,
        predictions: list[Any],
        model_container: ModelContainer,
    ) -> list[dict[str, Any]]:
        """Get model explanations."""
        try:
            # This is a placeholder for model explanation logic
            # In practice, you'd integrate with SHAP, LIME, or other explanation libraries
            explanations = []

            for i, prediction in enumerate(predictions):
                explanations.append(
                    {
                        "prediction_index": i,
                        "explanation_type": "placeholder",
                        "explanation": "Feature importance not yet implemented",
                    }
                )

            return explanations

        except Exception as error:
            logger.error("Failed to generate explanations", error=str(error))
            # Don't fail the request if explanations fail
            return []

    async def _health_check_loop(self) -> None:
        """Periodic health check for loaded models."""
        while True:
            try:
                await asyncio.sleep(self.config.health_check_interval_seconds)
                await self._perform_health_checks()
            except asyncio.CancelledError:
                break
            except Exception as error:
                logger.error("Health check failed", error=str(error))

    async def _perform_health_checks(self) -> None:
        """Perform health checks on all loaded models."""
        async with self.cache_lock:
            unhealthy_models = []

            for cache_key, model_container in self.model_cache.items():
                try:
                    # Simple health check - try to make a dummy prediction
                    if model_container.input_example is not None:
                        # Use input example for health check
                        test_input = model_container.input_example
                    else:
                        # Skip health check if no input example
                        continue

                    # This would be a real health check prediction
                    # For now, just mark as healthy if no recent failures
                    if model_container.consecutive_failures == 0:
                        model_container.healthy = True

                except Exception as error:
                    model_container.record_failure(error)
                    if not model_container.healthy:
                        unhealthy_models.append(cache_key)

                    logger.warning(
                        "Model health check failed",
                        model_name=model_container.name,
                        model_version=model_container.version,
                        error=str(error),
                    )

            # Remove unhealthy models from cache
            for cache_key in unhealthy_models:
                model_container = self.model_cache.pop(cache_key)
                logger.warning(
                    "Removed unhealthy model from cache",
                    model_name=model_container.name,
                    model_version=model_container.version,
                    consecutive_failures=model_container.consecutive_failures,
                )

    def get_server_stats(self) -> dict[str, Any]:
        """Get server statistics."""
        uptime_seconds = (datetime.now(timezone.utc) - self.start_time).total_seconds()

        return {
            "uptime_seconds": uptime_seconds,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate": self.error_count / max(self.request_count, 1),
            "loaded_models": len(self.model_cache),
            "cache_utilization": len(self.model_cache) / self.config.model_cache_size,
            "healthy_models": sum(
                1 for model in self.model_cache.values() if model.healthy
            ),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    def get_model_stats(self, model_name: str) -> dict[str, Any] | None:
        """Get statistics for a specific model."""
        for model_container in self.model_cache.values():
            if model_container.name == model_name:
                return {
                    "name": model_container.name,
                    "version": model_container.version,
                    "stage": model_container.stage,
                    "healthy": model_container.healthy,
                    "usage_count": model_container.usage_count,
                    "failure_count": model_container.failure_count,
                    "consecutive_failures": model_container.consecutive_failures,
                    "age_seconds": model_container.age_seconds,
                    "idle_seconds": model_container.idle_seconds,
                    "last_used": model_container.last_used.isoformat(),
                    "last_failure": model_container.last_failure.isoformat()
                    if model_container.last_failure
                    else None,
                }
        return None
