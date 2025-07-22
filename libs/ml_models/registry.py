"""ML Model Registry for centralized model management and versioning."""

import os
from datetime import datetime, timezone
from typing import Any

import mlflow
import structlog
from mlflow.entities.model_registry import ModelVersion
from pydantic import BaseModel, Field

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig

logger = structlog.get_logger(__name__)


class ModelRegistryConfig(BaseConfig):
    """Configuration for ML model registry."""

    # MLflow server configuration
    mlflow_tracking_uri: str = Field(
        default="sqlite:///mlflow.db", description="MLflow tracking server URI"
    )
    mlflow_artifact_root: str = Field(
        default="./mlartifacts", description="Root directory for MLflow artifacts"
    )
    mlflow_registry_uri: str = Field(
        default="sqlite:///mlflow.db", description="MLflow model registry URI"
    )

    # Model registry settings
    default_experiment_name: str = Field(
        default="analytics_ml_experiments",
        description="Default experiment name for model tracking",
    )
    model_lifecycle_stages: list[str] = Field(
        default=["None", "Staging", "Production", "Archived"],
        description="Available model lifecycle stages",
    )

    # Artifact storage
    artifact_storage_backend: str = Field(
        default="local", description="Artifact storage backend (local, s3, gcs, azure)"
    )
    s3_bucket: str | None = Field(
        default=None, description="S3 bucket for artifact storage"
    )

    # Model validation settings
    require_model_signature: bool = Field(
        default=True, description="Require model signature for registration"
    )
    require_input_example: bool = Field(
        default=True, description="Require input example for model registration"
    )


class ModelMetadata(BaseModel):
    """Metadata for ML model registration."""

    name: str = Field(description="Model name")
    version: str = Field(description="Model version")
    description: str | None = Field(default=None, description="Model description")

    # Model details
    model_type: str = Field(
        description="Type of model (classification, regression, etc.)"
    )
    algorithm: str = Field(description="ML algorithm used")
    framework: str = Field(description="ML framework (sklearn, lightgbm, etc.)")

    # Performance metrics
    metrics: dict[str, float] = Field(
        default_factory=dict, description="Model performance metrics"
    )

    # Training metadata
    training_dataset_version: str | None = Field(
        default=None, description="Version of training dataset"
    )
    training_run_id: str | None = Field(
        default=None, description="MLflow run ID used for training"
    )
    training_duration_seconds: float | None = Field(
        default=None, description="Training duration in seconds"
    )

    # Model artifacts
    model_artifact_uri: str | None = Field(
        default=None, description="URI of model artifacts"
    )
    model_size_bytes: int | None = Field(
        default=None, description="Model size in bytes"
    )

    # Validation results
    validation_results: dict[str, Any] = Field(
        default_factory=dict, description="Model validation test results"
    )

    # Lifecycle
    stage: str = Field(default="None", description="Model lifecycle stage")
    created_by: str | None = Field(default=None, description="User who created model")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Tags and labels
    tags: dict[str, str] = Field(
        default_factory=dict, description="Custom tags for model"
    )


class ModelRegistry:
    """Centralized model registry with MLflow backend."""

    def __init__(self, config: ModelRegistryConfig | None = None):
        """Initialize model registry."""
        self.config = config or ModelRegistryConfig()
        self.metrics = MLOpsMetrics()

        # Configure MLflow
        self._configure_mlflow()

        # Initialize registry
        self.client = mlflow.MlflowClient()

        logger.info(
            "Model registry initialized",
            tracking_uri=self.config.mlflow_tracking_uri,
            registry_uri=self.config.mlflow_registry_uri,
        )

    def _configure_mlflow(self) -> None:
        """Configure MLflow settings."""
        # Set MLflow tracking URI
        mlflow.set_tracking_uri(self.config.mlflow_tracking_uri)

        # Set artifact storage based on backend
        if self.config.artifact_storage_backend == "s3" and self.config.s3_bucket:
            os.environ["MLFLOW_S3_ENDPOINT_URL"] = f"s3://{self.config.s3_bucket}"

        # Set registry URI if different from tracking
        if self.config.mlflow_registry_uri != self.config.mlflow_tracking_uri:
            mlflow.set_registry_uri(self.config.mlflow_registry_uri)

    def register_model(
        self,
        model: Any,
        metadata: ModelMetadata,
        model_signature: Any | None = None,
        input_example: Any | None = None,
        conda_env: dict | None = None,
    ) -> str:
        """Register a model in the registry."""
        start_time = datetime.now(timezone.utc)

        try:
            # Validate requirements
            if self.config.require_model_signature and model_signature is None:
                raise ValueError("Model signature is required but not provided")

            if self.config.require_input_example and input_example is None:
                raise ValueError("Input example is required but not provided")

            # Create or get experiment
            experiment_id = self._ensure_experiment_exists(
                self.config.default_experiment_name
            )

            # Start MLflow run
            with mlflow.start_run(experiment_id=experiment_id) as run:
                # Log model parameters and metrics
                self._log_model_metadata(metadata)

                # Log model artifact
                model_uri = self._log_model(
                    model=model,
                    artifact_path="model",
                    signature=model_signature,
                    input_example=input_example,
                    conda_env=conda_env,
                    metadata=metadata,
                )

                # Register model in registry
                registered_model = self._register_model_version(
                    name=metadata.name,
                    source=model_uri,
                    run_id=run.info.run_id,
                    metadata=metadata,
                )

                # Update metadata with registration info
                metadata.model_artifact_uri = model_uri
                metadata.training_run_id = run.info.run_id

                # Record metrics
                self.metrics.record_model_registration(
                    model_name=metadata.name,
                    version=metadata.version,
                    stage=metadata.stage,
                    duration_seconds=(datetime.now(timezone.utc) - start_time).total_seconds(),
                )

                logger.info(
                    "Model registered successfully",
                    model_name=metadata.name,
                    version=registered_model.version,
                    run_id=run.info.run_id,
                    model_uri=model_uri,
                )

                return registered_model.version

        except Exception as error:
            self.metrics.record_model_registration_error(
                model_name=metadata.name,
                error_type=type(error).__name__,
                duration_seconds=(datetime.now(timezone.utc) - start_time).total_seconds(),
            )
            logger.error(
                "Failed to register model",
                model_name=metadata.name,
                error=str(error),
            )
            raise

    def _ensure_experiment_exists(self, experiment_name: str) -> str:
        """Ensure experiment exists and return its ID."""
        try:
            experiment = mlflow.get_experiment_by_name(experiment_name)
            if experiment is None:
                experiment_id = mlflow.create_experiment(experiment_name)
                logger.info(
                    "Created new experiment", name=experiment_name, id=experiment_id
                )
                return experiment_id
            return experiment.experiment_id
        except Exception as error:
            logger.error(
                "Failed to create/get experiment",
                experiment_name=experiment_name,
                error=str(error),
            )
            raise

    def _log_model_metadata(self, metadata: ModelMetadata) -> None:
        """Log model metadata to MLflow."""
        # Log parameters
        mlflow.log_param("model_type", metadata.model_type)
        mlflow.log_param("algorithm", metadata.algorithm)
        mlflow.log_param("framework", metadata.framework)
        mlflow.log_param("created_by", metadata.created_by)

        if metadata.training_dataset_version:
            mlflow.log_param(
                "training_dataset_version", metadata.training_dataset_version
            )

        # Log metrics
        for metric_name, metric_value in metadata.metrics.items():
            mlflow.log_metric(metric_name, metric_value)

        # Log tags
        for tag_key, tag_value in metadata.tags.items():
            mlflow.set_tag(tag_key, tag_value)

    def _log_model(
        self,
        model: Any,
        artifact_path: str,
        signature: Any | None,
        input_example: Any | None,
        conda_env: dict | None,
        metadata: ModelMetadata,
    ) -> str:
        """Log model to MLflow based on framework."""
        if metadata.framework.lower() == "sklearn":
            import mlflow.sklearn

            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path=artifact_path,
                signature=signature,
                input_example=input_example,
                conda_env=conda_env,
            )
        elif metadata.framework.lower() == "lightgbm":
            import mlflow.lightgbm

            mlflow.lightgbm.log_model(
                lgb_model=model,
                artifact_path=artifact_path,
                signature=signature,
                input_example=input_example,
                conda_env=conda_env,
            )
        else:
            # Fallback to generic python function model
            import mlflow.pyfunc

            mlflow.pyfunc.log_model(
                artifact_path=artifact_path,
                python_model=model,
                signature=signature,
                input_example=input_example,
                conda_env=conda_env,
            )

        # Return model URI
        run = mlflow.active_run()
        return f"runs:/{run.info.run_id}/{artifact_path}"

    def _register_model_version(
        self,
        name: str,
        source: str,
        run_id: str,
        metadata: ModelMetadata,
    ) -> ModelVersion:
        """Register model version in registry."""
        # Create registered model if it doesn't exist
        try:
            self.client.create_registered_model(
                name=name,
                description=metadata.description,
                tags=metadata.tags,
            )
            logger.info("Created new registered model", name=name)
        except Exception:
            # Model already exists
            logger.debug("Registered model already exists", name=name)

        # Create model version
        model_version = self.client.create_model_version(
            name=name,
            source=source,
            run_id=run_id,
            description=f"Version {metadata.version} - {metadata.description}",
            tags=metadata.tags,
        )

        return model_version

    def get_model(
        self, name: str, version: str | None = None, stage: str | None = None
    ) -> Any:
        """Load model from registry."""
        try:
            if version:
                model_uri = f"models:/{name}/{version}"
            elif stage:
                model_uri = f"models:/{name}/{stage}"
            else:
                # Get latest version
                latest_version = self.get_latest_model_version(name)
                model_uri = f"models:/{name}/{latest_version.version}"

            # Load model
            model = mlflow.pyfunc.load_model(model_uri)

            logger.info(
                "Model loaded successfully",
                name=name,
                version=version,
                stage=stage,
                model_uri=model_uri,
            )

            return model

        except Exception as error:
            logger.error(
                "Failed to load model",
                name=name,
                version=version,
                stage=stage,
                error=str(error),
            )
            raise

    def get_model_metadata(self, name: str, version: str) -> ModelMetadata:
        """Get model metadata from registry."""
        try:
            # Get model version
            model_version = self.client.get_model_version(name=name, version=version)

            # Get run details
            run = self.client.get_run(model_version.run_id)

            # Build metadata
            metadata = ModelMetadata(
                name=name,
                version=version,
                description=model_version.description,
                model_type=run.data.params.get("model_type", "unknown"),
                algorithm=run.data.params.get("algorithm", "unknown"),
                framework=run.data.params.get("framework", "unknown"),
                metrics={k: v for k, v in run.data.metrics.items()},
                training_dataset_version=run.data.params.get(
                    "training_dataset_version"
                ),
                training_run_id=model_version.run_id,
                model_artifact_uri=model_version.source,
                stage=model_version.current_stage,
                created_by=run.data.params.get("created_by"),
                created_at=datetime.fromtimestamp(
                    model_version.creation_timestamp / 1000, timezone.utc
                ),
                tags=dict(model_version.tags),
            )

            return metadata

        except Exception as error:
            logger.error(
                "Failed to get model metadata",
                name=name,
                version=version,
                error=str(error),
            )
            raise

    def get_latest_model_version(
        self, name: str, stage: str | None = None
    ) -> ModelVersion:
        """Get latest model version for given stage."""
        try:
            model_versions = self.client.get_latest_versions(
                name=name,
                stages=[stage] if stage else None,
            )

            if not model_versions:
                raise ValueError(f"No model versions found for {name}")

            return model_versions[0]

        except Exception as error:
            logger.error(
                "Failed to get latest model version",
                name=name,
                stage=stage,
                error=str(error),
            )
            raise

    def transition_model_stage(
        self, name: str, version: str, stage: str, archive_existing: bool = False
    ) -> ModelVersion:
        """Transition model to new stage."""
        try:
            # Validate stage
            if stage not in self.config.model_lifecycle_stages:
                raise ValueError(f"Invalid stage: {stage}")

            # Transition model
            model_version = self.client.transition_model_version_stage(
                name=name,
                version=version,
                stage=stage,
                archive_existing_versions=archive_existing,
            )

            # Record metrics
            self.metrics.record_model_stage_transition(
                model_name=name,
                version=version,
                from_stage=model_version.current_stage,
                to_stage=stage,
            )

            logger.info(
                "Model stage transitioned",
                name=name,
                version=version,
                new_stage=stage,
                archive_existing=archive_existing,
            )

            return model_version

        except Exception as error:
            logger.error(
                "Failed to transition model stage",
                name=name,
                version=version,
                stage=stage,
                error=str(error),
            )
            raise

    def list_models(self, max_results: int = 100) -> list[dict[str, Any]]:
        """List all registered models."""
        try:
            registered_models = self.client.search_registered_models(
                max_results=max_results
            )

            models = []
            for model in registered_models:
                # Get latest versions for each stage
                latest_versions = {}
                try:
                    for stage in ["Staging", "Production"]:
                        versions = self.client.get_latest_versions(
                            model.name, stages=[stage]
                        )
                        if versions:
                            latest_versions[stage] = versions[0].version
                except Exception:
                    pass

                models.append(
                    {
                        "name": model.name,
                        "description": model.description,
                        "creation_time": datetime.fromtimestamp(
                            model.creation_timestamp / 1000, timezone.utc
                        ),
                        "last_updated": datetime.fromtimestamp(
                            model.last_updated_timestamp / 1000, timezone.utc
                        ),
                        "latest_versions": latest_versions,
                        "tags": dict(model.tags) if model.tags else {},
                    }
                )

            return models

        except Exception as error:
            logger.error("Failed to list models", error=str(error))
            raise

    def delete_model(self, name: str) -> None:
        """Delete registered model."""
        try:
            self.client.delete_registered_model(name)

            logger.info("Model deleted", name=name)

        except Exception as error:
            logger.error(
                "Failed to delete model",
                name=name,
                error=str(error),
            )
            raise

    def search_model_versions(
        self,
        filter_string: str = "",
        max_results: int = 100,
        order_by: list[str] | None = None,
    ) -> list[ModelVersion]:
        """Search model versions with filters."""
        try:
            model_versions = self.client.search_model_versions(
                filter_string=filter_string,
                max_results=max_results,
                order_by=order_by,
            )

            return model_versions

        except Exception as error:
            logger.error(
                "Failed to search model versions",
                filter_string=filter_string,
                error=str(error),
            )
            raise

    def get_registry_stats(self) -> dict[str, Any]:
        """Get model registry statistics."""
        try:
            models = self.list_models()

            # Count models by stage
            stage_counts = {"None": 0, "Staging": 0, "Production": 0, "Archived": 0}
            total_versions = 0

            for model in models:
                for stage, version in model["latest_versions"].items():
                    if version:
                        stage_counts[stage] += 1
                        total_versions += 1

            # Get recent activity (last 7 days)
            recent_time = datetime.now(timezone.utc).timestamp() * 1000 - (
                7 * 24 * 60 * 60 * 1000
            )
            recent_models = [
                m for m in models if m["creation_time"].timestamp() * 1000 > recent_time
            ]

            return {
                "total_models": len(models),
                "total_versions": total_versions,
                "models_by_stage": stage_counts,
                "recent_models_7d": len(recent_models),
                "registry_health": "healthy" if len(models) > 0 else "empty",
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as error:
            logger.error("Failed to get registry stats", error=str(error))
            raise
