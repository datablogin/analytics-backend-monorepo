"""ML Experiment tracking functionality."""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from libs.analytics_core.models import Experiment, ExperimentRun, Metric, Param, RunTag


class ExperimentCreate(BaseModel):
    """Schema for creating a new experiment."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    tags: dict[str, str] | None = None
    artifact_location: str | None = None


class ExperimentResponse(BaseModel):
    """Schema for experiment response."""

    id: int
    name: str
    description: str | None
    tags: dict[str, Any] | None
    artifact_location: str | None
    lifecycle_stage: str
    created_at: datetime
    updated_at: datetime
    owner_id: int | None


class RunCreate(BaseModel):
    """Schema for creating a new experiment run."""

    name: str | None = None
    tags: dict[str, str] | None = None


class RunResponse(BaseModel):
    """Schema for experiment run response."""

    id: int
    experiment_id: int
    run_uuid: str
    name: str | None
    status: str
    start_time: datetime | None
    end_time: datetime | None
    artifact_uri: str | None
    created_at: datetime
    updated_at: datetime


class MetricCreate(BaseModel):
    """Schema for logging metrics."""

    key: str = Field(..., min_length=1, max_length=250)
    value: float
    step: int = 0


class ParamCreate(BaseModel):
    """Schema for logging parameters."""

    key: str = Field(..., min_length=1, max_length=250)
    value: str = Field(..., max_length=6000)


class ExperimentTracker:
    """ML Experiment tracking manager."""

    def __init__(self, db_session: AsyncSession, artifacts_root: str | None = None):
        """Initialize experiment tracker.

        Args:
            db_session: Database session
            artifacts_root: Root directory for storing artifacts
        """
        self.db = db_session
        self.artifacts_root = (
            Path(artifacts_root) if artifacts_root else Path("./mlruns")
        )
        self.artifacts_root.mkdir(exist_ok=True)

    async def create_experiment(
        self, experiment_data: ExperimentCreate, owner_id: int | None = None
    ) -> ExperimentResponse:
        """Create a new experiment."""
        # Set artifact location if not provided
        artifact_location = experiment_data.artifact_location
        if not artifact_location:
            artifact_location = str(self.artifacts_root / experiment_data.name)

        experiment = Experiment(
            name=experiment_data.name,
            description=experiment_data.description,
            tags=experiment_data.tags,
            artifact_location=artifact_location,
            owner_id=owner_id,
        )

        self.db.add(experiment)
        await self.db.commit()
        await self.db.refresh(experiment)

        # Create artifact directory
        Path(artifact_location).mkdir(parents=True, exist_ok=True)

        return ExperimentResponse(
            id=experiment.id,
            name=experiment.name,
            description=experiment.description,
            tags=experiment.tags,
            artifact_location=experiment.artifact_location,
            lifecycle_stage=experiment.lifecycle_stage,
            created_at=experiment.created_at,
            updated_at=experiment.updated_at,
            owner_id=experiment.owner_id,
        )

    async def list_experiments(
        self, limit: int = 100, offset: int = 0
    ) -> list[ExperimentResponse]:
        """List experiments with pagination."""
        result = await self.db.execute(
            select(Experiment)
            .where(Experiment.lifecycle_stage == "active")
            .offset(offset)
            .limit(limit)
            .order_by(Experiment.created_at.desc())
        )
        experiments = result.scalars().all()

        return [
            ExperimentResponse(
                id=exp.id,
                name=exp.name,
                description=exp.description,
                tags=exp.tags,
                artifact_location=exp.artifact_location,
                lifecycle_stage=exp.lifecycle_stage,
                created_at=exp.created_at,
                updated_at=exp.updated_at,
                owner_id=exp.owner_id,
            )
            for exp in experiments
        ]

    async def get_experiment(self, experiment_id: int) -> ExperimentResponse | None:
        """Get experiment by ID."""
        result = await self.db.execute(
            select(Experiment).where(Experiment.id == experiment_id)
        )
        experiment = result.scalar_one_or_none()

        if not experiment:
            return None

        return ExperimentResponse(
            id=experiment.id,
            name=experiment.name,
            description=experiment.description,
            tags=experiment.tags,
            artifact_location=experiment.artifact_location,
            lifecycle_stage=experiment.lifecycle_stage,
            created_at=experiment.created_at,
            updated_at=experiment.updated_at,
            owner_id=experiment.owner_id,
        )

    async def create_run(self, experiment_id: int, run_data: RunCreate) -> RunResponse:
        """Create a new experiment run."""
        # Generate unique run UUID
        run_uuid = uuid.uuid4().hex

        # Get experiment to build artifact URI
        result = await self.db.execute(
            select(Experiment).where(Experiment.id == experiment_id)
        )
        experiment = result.scalar_one_or_none()

        if not experiment:
            raise ValueError(f"Experiment {experiment_id} not found")

        artifact_uri = f"{experiment.artifact_location}/{run_uuid}/artifacts"

        run = ExperimentRun(
            experiment_id=experiment_id,
            run_uuid=run_uuid,
            name=run_data.name,
            status="RUNNING",
            start_time=datetime.utcnow(),
            artifact_uri=artifact_uri,
        )

        self.db.add(run)

        # Add tags if provided
        if run_data.tags:
            for key, value in run_data.tags.items():
                tag = RunTag(run_uuid=run_uuid, key=key, value=value)
                self.db.add(tag)

        await self.db.commit()
        await self.db.refresh(run)

        # Create artifact directory
        Path(artifact_uri).mkdir(parents=True, exist_ok=True)

        return RunResponse(
            id=run.id,
            experiment_id=run.experiment_id,
            run_uuid=run.run_uuid,
            name=run.name,
            status=run.status,
            start_time=run.start_time,
            end_time=run.end_time,
            artifact_uri=run.artifact_uri,
            created_at=run.created_at,
            updated_at=run.updated_at,
        )

    async def log_metric(self, run_uuid: str, metric: MetricCreate) -> None:
        """Log a metric for a run."""
        metric_obj = Metric(
            run_uuid=run_uuid,
            key=metric.key,
            value=metric.value,
            step=metric.step,
            timestamp=datetime.utcnow(),
        )

        self.db.add(metric_obj)
        await self.db.commit()

    async def log_param(self, run_uuid: str, param: ParamCreate) -> None:
        """Log a parameter for a run."""
        param_obj = Param(
            run_uuid=run_uuid,
            key=param.key,
            value=param.value,
        )

        self.db.add(param_obj)
        await self.db.commit()

    async def finish_run(self, run_uuid: str, status: str = "FINISHED") -> None:
        """Finish a run with specified status."""
        result = await self.db.execute(
            select(ExperimentRun).where(ExperimentRun.run_uuid == run_uuid)
        )
        run = result.scalar_one_or_none()

        if run:
            run.status = status
            run.end_time = datetime.utcnow()
            await self.db.commit()

    async def get_run_metrics(self, run_uuid: str) -> list[dict[str, Any]]:
        """Get all metrics for a run."""
        result = await self.db.execute(
            select(Metric).where(Metric.run_uuid == run_uuid).order_by(Metric.timestamp)
        )
        metrics = result.scalars().all()

        return [
            {
                "key": metric.key,
                "value": metric.value,
                "step": metric.step,
                "timestamp": metric.timestamp.isoformat(),
            }
            for metric in metrics
        ]

    async def get_run_params(self, run_uuid: str) -> dict[str, str]:
        """Get all parameters for a run."""
        result = await self.db.execute(select(Param).where(Param.run_uuid == run_uuid))
        params = result.scalars().all()

        return {param.key: param.value for param in params}

    async def list_runs(
        self, experiment_id: int, limit: int = 100, offset: int = 0
    ) -> list[RunResponse]:
        """List runs for an experiment."""
        result = await self.db.execute(
            select(ExperimentRun)
            .where(ExperimentRun.experiment_id == experiment_id)
            .offset(offset)
            .limit(limit)
            .order_by(ExperimentRun.created_at.desc())
        )
        runs = result.scalars().all()

        return [
            RunResponse(
                id=run.id,
                experiment_id=run.experiment_id,
                run_uuid=run.run_uuid,
                name=run.name,
                status=run.status,
                start_time=run.start_time,
                end_time=run.end_time,
                artifact_uri=run.artifact_uri,
                created_at=run.created_at,
                updated_at=run.updated_at,
            )
            for run in runs
        ]

    async def compare_runs(self, run_uuids: list[str]) -> dict[str, Any]:
        """Compare multiple runs with their metrics and parameters."""
        result = await self.db.execute(
            select(ExperimentRun)
            .options(
                selectinload(ExperimentRun.metrics),
                selectinload(ExperimentRun.params),
                selectinload(ExperimentRun.tags),
            )
            .where(ExperimentRun.run_uuid.in_(run_uuids))
        )
        runs = result.scalars().all()

        comparison: dict[str, Any] = {
            "runs": [],
            "metrics_comparison": {},
            "params_comparison": {},
        }

        all_metric_keys = set()
        all_param_keys = set()

        # Collect data for each run
        for run in runs:
            run_data = {
                "run_uuid": run.run_uuid,
                "name": run.name,
                "status": run.status,
                "start_time": run.start_time.isoformat() if run.start_time else None,
                "end_time": run.end_time.isoformat() if run.end_time else None,
                "metrics": {m.key: m.value for m in run.metrics},
                "params": {p.key: p.value for p in run.params},
                "tags": {t.key: t.value for t in run.tags},
            }
            comparison["runs"].append(run_data)

            all_metric_keys.update(run_data["metrics"].keys())  # type: ignore[attr-defined]
            all_param_keys.update(run_data["params"].keys())  # type: ignore[attr-defined]

        # Build comparison matrices
        for metric_key in all_metric_keys:
            comparison["metrics_comparison"][metric_key] = {
                run["run_uuid"]: run["metrics"].get(metric_key)
                for run in comparison["runs"]
            }

        for param_key in all_param_keys:
            comparison["params_comparison"][param_key] = {
                run["run_uuid"]: run["params"].get(param_key)
                for run in comparison["runs"]
            }

        return comparison
