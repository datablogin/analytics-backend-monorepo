"""MLflow-based experiment tracking for ML workflows."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import mlflow
import structlog
from mlflow.entities import Experiment
from pydantic import BaseModel, Field

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig

logger = structlog.get_logger(__name__)


class ExperimentConfig(BaseConfig):
    """Configuration for experiment tracking."""

    # MLflow configuration
    mlflow_tracking_uri: str = Field(
        default="sqlite:///mlflow.db", description="MLflow tracking server URI"
    )
    artifact_root: str = Field(
        default="./mlartifacts", description="Root directory for MLflow artifacts"
    )

    # Experiment settings
    auto_log: bool = Field(default=True, description="Enable MLflow autologging")
    log_system_metrics: bool = Field(
        default=True, description="Log system metrics during runs"
    )
    nested_run_support: bool = Field(
        default=True, description="Support nested MLflow runs"
    )

    # Artifact management
    log_model_artifacts: bool = Field(
        default=True, description="Automatically log model artifacts"
    )
    log_dataset_artifacts: bool = Field(
        default=False, description="Log dataset artifacts (can be large)"
    )
    artifact_compression: bool = Field(
        default=True, description="Compress artifacts for storage"
    )

    # Run management
    max_runs_per_experiment: int = Field(
        default=1000, description="Maximum runs to keep per experiment"
    )
    auto_cleanup_failed_runs: bool = Field(
        default=True, description="Auto cleanup failed runs after threshold"
    )


class ExperimentMetadata(BaseModel):
    """Metadata for ML experiments."""

    name: str = Field(description="Experiment name")
    description: str | None = Field(default=None, description="Experiment description")

    # Experiment categorization
    project: str | None = Field(default=None, description="Project name")
    team: str | None = Field(default=None, description="Team responsible")
    use_case: str | None = Field(default=None, description="Use case or application")

    # Experiment settings
    tags: dict[str, str] = Field(
        default_factory=dict, description="Custom tags for experiment"
    )

    # Lifecycle
    created_by: str | None = Field(
        default=None, description="User who created experiment"
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class RunMetadata(BaseModel):
    """Metadata for ML runs."""

    run_name: str | None = Field(default=None, description="Human-readable run name")
    description: str | None = Field(default=None, description="Run description")

    # Run categorization
    experiment_id: str = Field(description="Parent experiment ID")
    run_type: str = Field(
        default="training",
        description="Type of run (training, validation, testing, etc.)",
    )

    # Parameters and hyperparameters
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Model parameters and hyperparameters"
    )

    # Metrics to track
    metrics: dict[str, float] = Field(
        default_factory=dict, description="Performance metrics"
    )

    # System information
    environment: dict[str, str] = Field(
        default_factory=dict, description="Environment information"
    )

    # Tags and metadata
    tags: dict[str, str] = Field(
        default_factory=dict, description="Custom tags for run"
    )

    # Artifacts
    artifacts: list[str] = Field(
        default_factory=list, description="List of artifact paths to log"
    )

    # Lifecycle
    created_by: str | None = Field(default=None, description="User who created run")


class ExperimentTracker:
    """MLflow-based experiment tracking system."""

    def __init__(self, config: ExperimentConfig | None = None):
        """Initialize experiment tracker."""
        self.config = config or ExperimentConfig()
        self.metrics = MLOpsMetrics()

        # Configure MLflow
        self._configure_mlflow()

        # Validate MLflow connectivity
        self._validate_mlflow_connectivity()

        logger.info(
            "Experiment tracker initialized",
            tracking_uri=self.config.mlflow_tracking_uri,
            auto_log=self.config.auto_log,
        )

    def _configure_mlflow(self) -> None:
        """Configure MLflow settings."""
        # Set tracking URI
        mlflow.set_tracking_uri(self.config.mlflow_tracking_uri)

        # Configure autologging
        if self.config.auto_log:
            mlflow.sklearn.autolog()
            mlflow.lightgbm.autolog()

            # Enable system metrics logging
            if self.config.log_system_metrics:
                mlflow.enable_system_metrics_logging()

    def _validate_mlflow_connectivity(self) -> None:
        """Validate MLflow tracking server connectivity."""
        try:
            # Simple connectivity test - try to list experiments
            client = mlflow.MlflowClient()
            experiments = client.search_experiments(max_results=1)

            logger.info(
                "MLflow connectivity validated for experiment tracker",
                tracking_uri=self.config.mlflow_tracking_uri,
                available_experiments=len(experiments) if experiments else 0,
            )

        except Exception as error:
            logger.error(
                "MLflow connectivity validation failed for experiment tracker",
                tracking_uri=self.config.mlflow_tracking_uri,
                error=str(error),
            )
            logger.warning(
                "Experiment tracker may not function properly - "
                "check MLflow server availability"
            )

    def create_experiment(self, metadata: ExperimentMetadata) -> str:
        """Create new experiment."""
        try:
            # Check if experiment already exists
            existing_experiment = mlflow.get_experiment_by_name(metadata.name)
            if existing_experiment is not None:
                logger.info(
                    "Experiment already exists",
                    name=metadata.name,
                    experiment_id=existing_experiment.experiment_id,
                )
                return existing_experiment.experiment_id

            # Create experiment
            experiment_id = mlflow.create_experiment(
                name=metadata.name,
                artifact_location=f"{self.config.artifact_root}/{metadata.name}",
                tags=metadata.tags,
            )

            # Record metrics
            self.metrics.record_experiment_creation(
                experiment_name=metadata.name,
                experiment_id=experiment_id,
                created_by=metadata.created_by,
            )

            logger.info(
                "Experiment created",
                name=metadata.name,
                experiment_id=experiment_id,
                created_by=metadata.created_by,
            )

            return experiment_id

        except Exception as error:
            logger.error(
                "Failed to create experiment",
                name=metadata.name,
                error=str(error),
            )
            raise

    def start_run(
        self,
        experiment_name: str | None = None,
        experiment_id: str | None = None,
        metadata: RunMetadata | None = None,
        nested: bool = False,
    ) -> str:
        """Start new MLflow run."""
        try:
            # Determine experiment
            if experiment_name:
                experiment = mlflow.get_experiment_by_name(experiment_name)
                if experiment is None:
                    raise ValueError(f"Experiment '{experiment_name}' not found")
                exp_id = experiment.experiment_id
            elif experiment_id:
                exp_id = experiment_id
            else:
                raise ValueError(
                    "Either experiment_name or experiment_id must be provided"
                )

            # Start run
            run = mlflow.start_run(
                experiment_id=exp_id,
                run_name=metadata.run_name if metadata else None,
                nested=nested,
                tags=metadata.tags if metadata else None,
            )

            # Log run metadata if provided
            if metadata:
                self._log_run_metadata(metadata)

            logger.info(
                "MLflow run started",
                run_id=run.info.run_id,
                experiment_id=exp_id,
                run_name=metadata.run_name if metadata else None,
                nested=nested,
            )

            return run.info.run_id

        except Exception as error:
            logger.error(
                "Failed to start run",
                experiment_name=experiment_name,
                experiment_id=experiment_id,
                error=str(error),
            )
            raise

    def _log_run_metadata(self, metadata: RunMetadata) -> None:
        """Log run metadata to current MLflow run."""
        # Log parameters
        for param_name, param_value in metadata.parameters.items():
            mlflow.log_param(param_name, param_value)

        # Log initial metrics
        for metric_name, metric_value in metadata.metrics.items():
            mlflow.log_metric(metric_name, metric_value)

        # Log environment information
        for env_key, env_value in metadata.environment.items():
            mlflow.set_tag(f"env.{env_key}", env_value)

        # Log additional tags
        for tag_key, tag_value in metadata.tags.items():
            mlflow.set_tag(tag_key, tag_value)

        # Set run type and description
        mlflow.set_tag("run_type", metadata.run_type)
        if metadata.description:
            mlflow.set_tag("description", metadata.description)
        if metadata.created_by:
            mlflow.set_tag("created_by", metadata.created_by)

    def log_parameters(self, parameters: dict[str, Any]) -> None:
        """Log parameters to current run."""
        try:
            for name, value in parameters.items():
                mlflow.log_param(name, value)

            logger.debug("Parameters logged", count=len(parameters))

        except Exception as error:
            logger.error("Failed to log parameters", error=str(error))
            raise

    def log_metrics(self, metrics: dict[str, float], step: int | None = None) -> None:
        """Log metrics to current run."""
        try:
            for name, value in metrics.items():
                mlflow.log_metric(name, value, step=step)

            logger.debug("Metrics logged", count=len(metrics), step=step)

        except Exception as error:
            logger.error("Failed to log metrics", error=str(error))
            raise

    def log_metric_history(self, metric_name: str, values: list[float]) -> None:
        """Log metric history over time."""
        try:
            for step, value in enumerate(values):
                mlflow.log_metric(metric_name, value, step=step)

            logger.debug("Metric history logged", metric=metric_name, steps=len(values))

        except Exception as error:
            logger.error(
                "Failed to log metric history",
                metric=metric_name,
                error=str(error),
            )
            raise

    def log_artifacts(self, artifact_paths: list[str | Path]) -> None:
        """Log artifacts to current run."""
        try:
            for artifact_path in artifact_paths:
                path = Path(artifact_path)
                if path.is_file():
                    mlflow.log_artifact(str(path))
                elif path.is_dir():
                    mlflow.log_artifacts(str(path))
                else:
                    logger.warning("Artifact path not found", path=str(path))

            logger.debug("Artifacts logged", count=len(artifact_paths))

        except Exception as error:
            logger.error("Failed to log artifacts", error=str(error))
            raise

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        signature: Any | None = None,
        input_example: Any | None = None,
        registered_model_name: str | None = None,
    ) -> None:
        """Log model to current run."""
        try:
            if hasattr(model, "predict"):
                # Try to determine model type
                model_type = type(model).__module__

                if "sklearn" in model_type:
                    import mlflow.sklearn

                    mlflow.sklearn.log_model(
                        sk_model=model,
                        artifact_path=artifact_path,
                        signature=signature,
                        input_example=input_example,
                        registered_model_name=registered_model_name,
                    )
                elif "lightgbm" in model_type:
                    import mlflow.lightgbm

                    mlflow.lightgbm.log_model(
                        lgb_model=model,
                        artifact_path=artifact_path,
                        signature=signature,
                        input_example=input_example,
                        registered_model_name=registered_model_name,
                    )
                else:
                    # Fallback to pyfunc
                    import mlflow.pyfunc

                    mlflow.pyfunc.log_model(
                        artifact_path=artifact_path,
                        python_model=model,
                        signature=signature,
                        input_example=input_example,
                        registered_model_name=registered_model_name,
                    )
            else:
                logger.warning("Model does not have predict method, using pyfunc")
                import mlflow.pyfunc

                mlflow.pyfunc.log_model(
                    artifact_path=artifact_path,
                    python_model=model,
                    signature=signature,
                    input_example=input_example,
                    registered_model_name=registered_model_name,
                )

            logger.info(
                "Model logged",
                artifact_path=artifact_path,
                registered_model_name=registered_model_name,
            )

        except Exception as error:
            logger.error(
                "Failed to log model",
                artifact_path=artifact_path,
                error=str(error),
            )
            raise

    def log_dataset(self, dataset: Any, name: str, format: str = "parquet") -> None:
        """Log dataset as artifact."""
        if not self.config.log_dataset_artifacts:
            logger.debug("Dataset logging disabled")
            return

        try:
            with tempfile.NamedTemporaryFile(
                suffix=f".{format}", delete=False
            ) as tmp_file:
                if format.lower() == "parquet":
                    dataset.to_parquet(tmp_file.name)
                elif format.lower() == "csv":
                    dataset.to_csv(tmp_file.name, index=False)
                elif format.lower() == "json":
                    dataset.to_json(tmp_file.name, orient="records")
                else:
                    raise ValueError(f"Unsupported format: {format}")

                mlflow.log_artifact(tmp_file.name, f"datasets/{name}.{format}")

                # Clean up temp file
                Path(tmp_file.name).unlink()

            logger.debug("Dataset logged", name=name, format=format)

        except Exception as error:
            logger.error(
                "Failed to log dataset",
                name=name,
                format=format,
                error=str(error),
            )
            raise

    def log_confusion_matrix(
        self, y_true: Any, y_pred: Any, artifact_path: str = "plots"
    ) -> None:
        """Log confusion matrix plot."""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            from sklearn.metrics import confusion_matrix

            # Calculate confusion matrix
            cm = confusion_matrix(y_true, y_pred)

            # Create plot
            plt.figure(figsize=(8, 6))
            sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
            plt.title("Confusion Matrix")
            plt.ylabel("True Label")
            plt.xlabel("Predicted Label")

            # Save and log
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
                plt.savefig(tmp_file.name, dpi=150, bbox_inches="tight")
                mlflow.log_artifact(
                    tmp_file.name, f"{artifact_path}/confusion_matrix.png"
                )
                Path(tmp_file.name).unlink()

            plt.close()

            logger.debug("Confusion matrix logged")

        except Exception as error:
            logger.error("Failed to log confusion matrix", error=str(error))
            raise

    def log_feature_importance(
        self, model: Any, feature_names: list[str], artifact_path: str = "plots"
    ) -> None:
        """Log feature importance plot."""
        try:
            import matplotlib.pyplot as plt
            import pandas as pd

            # Get feature importance
            if hasattr(model, "feature_importances_"):
                importances = model.feature_importances_
            elif hasattr(model, "coef_"):
                importances = abs(model.coef_.flatten())
            else:
                logger.warning("Model does not have feature importance")
                return

            # Create DataFrame
            importance_df = pd.DataFrame(
                {
                    "feature": feature_names,
                    "importance": importances,
                }
            ).sort_values("importance", ascending=False)

            # Create plot
            plt.figure(figsize=(10, max(6, len(feature_names) * 0.3)))
            plt.barh(range(len(importance_df)), importance_df["importance"])
            plt.yticks(range(len(importance_df)), importance_df["feature"])
            plt.xlabel("Feature Importance")
            plt.title("Feature Importance")
            plt.tight_layout()

            # Save and log
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
                plt.savefig(tmp_file.name, dpi=150, bbox_inches="tight")
                mlflow.log_artifact(
                    tmp_file.name, f"{artifact_path}/feature_importance.png"
                )
                Path(tmp_file.name).unlink()

            plt.close()

            # Also log as table
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as tmp_file:
                importance_df.to_json(tmp_file.name, orient="records", indent=2)
                mlflow.log_artifact(
                    tmp_file.name, f"{artifact_path}/feature_importance.json"
                )
                Path(tmp_file.name).unlink()

            logger.debug("Feature importance logged")

        except Exception as error:
            logger.error("Failed to log feature importance", error=str(error))
            raise

    def end_run(self, status: str = "FINISHED") -> None:
        """End current MLflow run."""
        try:
            mlflow.end_run(status=status)
            logger.info("MLflow run ended", status=status)

        except Exception as error:
            logger.error("Failed to end run", error=str(error))
            raise

    def get_experiment(self, name: str) -> Experiment | None:
        """Get experiment by name."""
        try:
            return mlflow.get_experiment_by_name(name)
        except Exception as error:
            logger.error("Failed to get experiment", name=name, error=str(error))
            return None

    def list_experiments(self, view_type: str = "ACTIVE_ONLY") -> list[Experiment]:
        """List all experiments."""
        try:
            client = mlflow.MlflowClient()
            experiments = client.search_experiments(
                view_type=mlflow.entities.ViewType.ACTIVE_ONLY
            )
            return experiments

        except Exception as error:
            logger.error("Failed to list experiments", error=str(error))
            raise

    def search_runs(
        self,
        experiment_names: list[str] | None = None,
        filter_string: str = "",
        run_view_type: str = "ACTIVE_ONLY",
        max_results: int = 100,
        order_by: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Search runs across experiments."""
        try:
            client = mlflow.MlflowClient()

            # Get experiment IDs
            experiment_ids = []
            if experiment_names:
                for name in experiment_names:
                    exp = mlflow.get_experiment_by_name(name)
                    if exp:
                        experiment_ids.append(exp.experiment_id)

            # Search runs
            runs = client.search_runs(
                experiment_ids=experiment_ids,
                filter_string=filter_string,
                run_view_type=mlflow.entities.ViewType.ACTIVE_ONLY,
                max_results=max_results,
                order_by=order_by,
            )

            # Convert to dict format
            run_data = []
            for run in runs:
                run_data.append(
                    {
                        "run_id": run.info.run_id,
                        "experiment_id": run.info.experiment_id,
                        "status": run.info.status,
                        "start_time": datetime.fromtimestamp(
                            run.info.start_time / 1000, timezone.utc
                        ),
                        "end_time": datetime.fromtimestamp(
                            run.info.end_time / 1000, timezone.utc
                        )
                        if run.info.end_time
                        else None,
                        "parameters": dict(run.data.params),
                        "metrics": dict(run.data.metrics),
                        "tags": dict(run.data.tags),
                    }
                )

            return run_data

        except Exception as error:
            logger.error("Failed to search runs", error=str(error))
            raise

    def compare_runs(self, run_ids: list[str]) -> dict[str, Any]:
        """Compare multiple runs."""
        try:
            client = mlflow.MlflowClient()

            runs = [client.get_run(run_id) for run_id in run_ids]

            # Extract comparison data
            comparison = {
                "runs": [],
                "common_parameters": {},
                "different_parameters": {},
                "metrics_comparison": {},
            }

            all_params = {}
            all_metrics = {}

            for run in runs:
                run_data = {
                    "run_id": run.info.run_id,
                    "status": run.info.status,
                    "start_time": datetime.fromtimestamp(
                        run.info.start_time / 1000, timezone.utc
                    ),
                    "parameters": dict(run.data.params),
                    "metrics": dict(run.data.metrics),
                    "tags": dict(run.data.tags),
                }
                comparison["runs"].append(run_data)

                # Collect all params and metrics
                for param, value in run.data.params.items():
                    if param not in all_params:
                        all_params[param] = []
                    all_params[param].append(value)

                for metric, value in run.data.metrics.items():
                    if metric not in all_metrics:
                        all_metrics[metric] = []
                    all_metrics[metric].append(value)

            # Identify common vs different parameters
            for param, values in all_params.items():
                if len(set(values)) == 1:
                    comparison["common_parameters"][param] = values[0]
                else:
                    comparison["different_parameters"][param] = values

            # Add metrics comparison
            comparison["metrics_comparison"] = all_metrics

            return comparison

        except Exception as error:
            logger.error("Failed to compare runs", run_ids=run_ids, error=str(error))
            raise

    def delete_experiment(self, experiment_id: str) -> None:
        """Delete experiment."""
        try:
            client = mlflow.MlflowClient()
            client.delete_experiment(experiment_id)

            logger.info("Experiment deleted", experiment_id=experiment_id)

        except Exception as error:
            logger.error(
                "Failed to delete experiment",
                experiment_id=experiment_id,
                error=str(error),
            )
            raise

    def cleanup_failed_runs(self, experiment_name: str, days_old: int = 7) -> int:
        """Clean up failed runs older than specified days."""
        if not self.config.auto_cleanup_failed_runs:
            logger.debug("Auto cleanup disabled")
            return 0

        try:
            cutoff_time = datetime.now(timezone.utc).timestamp() * 1000 - (
                days_old * 24 * 60 * 60 * 1000
            )

            # Search for failed runs
            runs = self.search_runs(
                experiment_names=[experiment_name],
                filter_string="attribute.status = 'FAILED'",
                max_results=1000,
            )

            # Filter old runs
            old_failed_runs = [
                run
                for run in runs
                if run["start_time"].timestamp() * 1000 < cutoff_time
            ]

            # Delete old failed runs
            client = mlflow.MlflowClient()
            deleted_count = 0

            for run in old_failed_runs:
                try:
                    client.delete_run(run["run_id"])
                    deleted_count += 1
                except Exception as delete_error:
                    logger.warning(
                        "Failed to delete run",
                        run_id=run["run_id"],
                        error=str(delete_error),
                    )

            logger.info(
                "Cleaned up failed runs",
                experiment_name=experiment_name,
                deleted_count=deleted_count,
                days_old=days_old,
            )

            return deleted_count

        except Exception as error:
            logger.error(
                "Failed to cleanup failed runs",
                experiment_name=experiment_name,
                error=str(error),
            )
            return 0
