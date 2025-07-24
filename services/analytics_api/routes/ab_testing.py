"""A/B Testing API endpoints for experiment management."""

from datetime import datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from libs.analytics_core.ab_testing_async import (
    ABTestExperiment,
    AsyncABTestingEngine,
    ExperimentObjective,
    ExperimentStatus,
    ExperimentVariant,
    StoppingRule,
)
from libs.analytics_core.auth import get_current_user
from libs.analytics_core.database import get_db_session
from libs.analytics_core.models import User
from libs.analytics_core.statistics import TestType
from libs.api_common.response_models import APIMetadata, StandardResponse

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/ab-testing", tags=["A/B Testing"])


# Dependency to provide A/B testing engine
def get_ab_testing_engine() -> AsyncABTestingEngine:
    """Get A/B testing engine instance."""
    return AsyncABTestingEngine(session_factory=None)


# Request/Response Models
class CreateExperimentRequest(BaseModel):
    """Request to create new A/B test experiment."""

    name: str = Field(description="Experiment name", min_length=1, max_length=255)
    description: str | None = Field(
        default=None, description="Experiment description", max_length=10000
    )
    feature_flag_key: str = Field(
        description="Associated feature flag key", min_length=1, max_length=255
    )
    objective: str = Field(description="Primary objective", min_length=1)
    hypothesis: str = Field(description="Experiment hypothesis", min_length=1)
    variants: list[ExperimentVariant] = Field(
        description="Experiment variants", min_length=2, max_length=10
    )
    primary_metric: str = Field(
        description="Primary success metric", min_length=1, max_length=100
    )
    secondary_metrics: list[str] = Field(default_factory=list, max_length=20)
    guardrail_metrics: list[str] = Field(default_factory=list, max_length=20)
    target_audience: dict[str, Any] = Field(default_factory=dict, max_length=50)
    exclusion_rules: dict[str, Any] = Field(default_factory=dict, max_length=50)
    significance_level: float = Field(default=0.05, ge=0.001, le=0.2)
    power: float = Field(default=0.8, ge=0.5, le=0.99)
    minimum_detectable_effect: float | None = Field(default=None, ge=0.001, le=10.0)
    stopping_rules: StoppingRule | None = Field(default=None)


class StartExperimentRequest(BaseModel):
    """Request to start experiment."""

    start_date: datetime | None = Field(default=None)
    end_date: datetime | None = Field(default=None)


class AssignVariantRequest(BaseModel):
    """Request to assign user to variant."""

    user_id: str = Field(description="User ID", min_length=1, max_length=255)
    user_attributes: dict[str, Any] = Field(default_factory=dict, max_length=100)


class TrackEventRequest(BaseModel):
    """Request to track experiment event."""

    user_id: str = Field(description="User ID", min_length=1, max_length=255)
    event_type: str = Field(description="Event type", min_length=1, max_length=100)
    event_value: float | str | bool | None = Field(default=None)
    properties: dict[str, Any] = Field(default_factory=dict, max_length=100)


class AnalyzeExperimentRequest(BaseModel):
    """Request to analyze experiment."""

    test_type: TestType = Field(default=TestType.TWO_SAMPLE_TTEST)


class ExperimentSummary(BaseModel):
    """Summary of experiment results."""

    id: str
    name: str
    status: ExperimentStatus
    feature_flag_key: str
    variants: list[str]
    start_date: datetime | None
    end_date: datetime | None
    created_by: str | None
    winner: str | None
    confidence: float | None


@router.post("", response_model=StandardResponse[dict])
async def create_experiment(
    request: Request,
    experiment_request: CreateExperimentRequest,
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Create new A/B test experiment."""
    try:
        # Create experiment object
        experiment = ABTestExperiment(
            name=experiment_request.name,
            description=experiment_request.description,
            feature_flag_key=experiment_request.feature_flag_key,
            objective=ExperimentObjective(experiment_request.objective),
            hypothesis=experiment_request.hypothesis,
            variants=experiment_request.variants,
            primary_metric=experiment_request.primary_metric,
            secondary_metrics=experiment_request.secondary_metrics,
            guardrail_metrics=experiment_request.guardrail_metrics,
            target_audience=experiment_request.target_audience,
            exclusion_rules=experiment_request.exclusion_rules,
            significance_level=experiment_request.significance_level,
            power=experiment_request.power,
            minimum_detectable_effect=experiment_request.minimum_detectable_effect,
            stopping_rules=experiment_request.stopping_rules,
            created_by=current_user.username,
        )

        # Create experiment
        created_experiment = await engine.create_experiment(experiment, db)

        logger.info(
            "A/B test experiment created via API",
            experiment_id=created_experiment.id,
            name=created_experiment.name,
            user=current_user.username,
        )

        return StandardResponse(
            success=True,
            data={
                "experiment_id": created_experiment.id,
                "name": created_experiment.name,
                "status": created_experiment.status.value,
                "feature_flag_key": created_experiment.feature_flag_key,
                "variants": [v.name for v in created_experiment.variants],
                "mlflow_experiment_id": created_experiment.mlflow_experiment_id,
            },
            message="A/B test experiment created successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except ValueError as e:
        logger.error("Invalid experiment configuration", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to create experiment", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to create experiment: {str(e)}"
        )


@router.post("/{experiment_id}/start", response_model=StandardResponse[dict])
async def start_experiment(
    request: Request,
    experiment_id: str,
    start_request: StartExperimentRequest,
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Start an A/B test experiment."""
    try:
        # Update experiment dates if provided
        experiment = await engine.get_experiment(
            experiment_uuid=experiment_id, session=db
        )
        if not experiment:
            raise HTTPException(status_code=404, detail="Experiment not found")

        if start_request.start_date:
            experiment.start_date = start_request.start_date
        if start_request.end_date:
            experiment.end_date = start_request.end_date

        # Start experiment
        success = await engine.start_experiment(
            experiment_uuid=experiment_id, session=db
        )

        if not success:
            raise HTTPException(status_code=400, detail="Failed to start experiment")

        logger.info(
            "A/B test experiment started via API",
            experiment_id=experiment_id,
            user=current_user.username,
        )

        return StandardResponse(
            success=True,
            data={
                "experiment_id": experiment_id,
                "status": "running",
                "start_date": experiment.start_date.isoformat()
                if experiment.start_date
                else None,
                "end_date": experiment.end_date.isoformat()
                if experiment.end_date
                else None,
            },
            message="Experiment started successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Failed to start experiment", experiment_id=experiment_id, error=str(e)
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to start experiment: {str(e)}"
        )


@router.post("/{experiment_id}/assign", response_model=StandardResponse[dict])
async def assign_user_to_variant(
    request: Request,
    experiment_id: str,
    assign_request: AssignVariantRequest,
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Assign user to experiment variant."""
    try:
        variant = await engine.assign_user_to_variant(
            experiment_uuid=experiment_id,
            user_id=assign_request.user_id,
            session=db,
            user_attributes=assign_request.user_attributes,
        )

        if not variant:
            return StandardResponse(
                success=False,
                data={
                    "experiment_id": experiment_id,
                    "user_id": assign_request.user_id,
                    "variant": None,
                    "assigned": False,
                },
                message="User not eligible for experiment or experiment not running",
                metadata=APIMetadata(
                    request_id=getattr(request.state, "request_id", "unknown"),
                    version="v1",
                    environment="development",
                ),
            )

        # Get feature value for assigned variant
        feature_value = await engine.get_feature_value(
            experiment_uuid=experiment_id,
            user_id=assign_request.user_id,
            session=db,
            user_attributes=assign_request.user_attributes,
        )

        return StandardResponse(
            success=True,
            data={
                "experiment_id": experiment_id,
                "user_id": assign_request.user_id,
                "variant": variant,
                "feature_value": feature_value,
                "assigned": True,
            },
            message="User assigned to variant successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        logger.error(
            "Failed to assign user to variant",
            experiment_id=experiment_id,
            user_id=assign_request.user_id,
            error=str(e),
        )
        raise HTTPException(status_code=500, detail=f"Failed to assign user: {str(e)}")


@router.post("/{experiment_id}/events", response_model=StandardResponse[dict])
async def track_experiment_event(
    request: Request,
    experiment_id: str,
    event_request: TrackEventRequest,
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Track event for experiment analysis."""
    try:
        success = await engine.track_event(
            experiment_uuid=experiment_id,
            user_id=event_request.user_id,
            event_type=event_request.event_type,
            session=db,
            event_value=event_request.event_value,
            properties=event_request.properties,
        )

        if not success:
            return StandardResponse(
                success=False,
                data={
                    "experiment_id": experiment_id,
                    "user_id": event_request.user_id,
                    "event_type": event_request.event_type,
                    "tracked": False,
                },
                message="Event not tracked - user not assigned to experiment",
                metadata=APIMetadata(
                    request_id=getattr(request.state, "request_id", "unknown"),
                    version="v1",
                    environment="development",
                ),
            )

        return StandardResponse(
            success=True,
            data={
                "experiment_id": experiment_id,
                "user_id": event_request.user_id,
                "event_type": event_request.event_type,
                "event_value": event_request.event_value,
                "tracked": True,
                "timestamp": datetime.now().isoformat(),
            },
            message="Event tracked successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        logger.error(
            "Failed to track event",
            experiment_id=experiment_id,
            user_id=event_request.user_id,
            event_type=event_request.event_type,
            error=str(e),
        )
        raise HTTPException(status_code=500, detail=f"Failed to track event: {str(e)}")


@router.post("/{experiment_id}/analyze", response_model=StandardResponse[dict])
async def analyze_experiment(
    request: Request,
    experiment_id: str,
    analyze_request: AnalyzeExperimentRequest,
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Analyze experiment results."""
    try:
        results = await engine.analyze_experiment(
            experiment_uuid=experiment_id,
            session=db,
            test_type=analyze_request.test_type.value,
        )

        logger.info(
            "A/B test experiment analyzed via API",
            experiment_id=experiment_id,
            test_type=analyze_request.test_type.value,
            user=current_user.username,
        )

        return StandardResponse(
            success=True,
            data=results,
            message="Experiment analysis completed",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        logger.error(
            "Failed to analyze experiment",
            experiment_id=experiment_id,
            error=str(e),
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to analyze experiment: {str(e)}"
        )


@router.get("/{experiment_id}/stopping-criteria", response_model=StandardResponse[dict])
async def check_stopping_criteria(
    request: Request,
    experiment_id: str,
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Check if experiment should be stopped."""
    try:
        criteria_result = await engine.check_stopping_criteria(
            experiment_uuid=experiment_id, session=db
        )

        return StandardResponse(
            success=True,
            data=criteria_result,
            message="Stopping criteria checked",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        logger.error(
            "Failed to check stopping criteria",
            experiment_id=experiment_id,
            error=str(e),
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to check stopping criteria: {str(e)}"
        )


@router.post("/{experiment_id}/stop", response_model=StandardResponse[dict])
async def stop_experiment(
    request: Request,
    experiment_id: str,
    reason: str | None = Query(None, description="Reason for stopping"),
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Stop a running experiment."""
    try:
        success = await engine.stop_experiment(
            experiment_uuid=experiment_id, reason=reason, session=db
        )

        if not success:
            raise HTTPException(status_code=400, detail="Failed to stop experiment")

        experiment = await engine.get_experiment(
            experiment_uuid=experiment_id, session=db
        )

        logger.info(
            "A/B test experiment stopped via API",
            experiment_id=experiment_id,
            reason=reason,
            user=current_user.username,
        )

        return StandardResponse(
            success=True,
            data={
                "experiment_id": experiment_id,
                "status": "completed",
                "reason": reason,
                "winner": experiment.winner if experiment else None,
                "confidence": experiment.confidence if experiment else None,
                "end_date": experiment.end_date.isoformat()
                if experiment and experiment.end_date
                else None,
            },
            message="Experiment stopped successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Failed to stop experiment", experiment_id=experiment_id, error=str(e)
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to stop experiment: {str(e)}"
        )


@router.get("/{experiment_id}", response_model=StandardResponse[dict])
async def get_experiment(
    request: Request,
    experiment_id: str,
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Get experiment details."""
    try:
        experiment = await engine.get_experiment(
            experiment_uuid=experiment_id, session=db
        )

        if not experiment:
            raise HTTPException(status_code=404, detail="Experiment not found")

        return StandardResponse(
            success=True,
            data=experiment.model_dump(),
            message="Experiment retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Failed to get experiment", experiment_id=experiment_id, error=str(e)
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to get experiment: {str(e)}"
        )


@router.get("", response_model=StandardResponse[list[ExperimentSummary]])
async def list_experiments(
    request: Request,
    status: ExperimentStatus | None = Query(None, description="Filter by status"),
    created_by: str | None = Query(None, description="Filter by creator"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[list[ExperimentSummary]]:
    """List A/B test experiments."""
    try:
        experiments = await engine.list_experiments(
            session=db, status=status.value if status else None, created_by=created_by
        )

        # Convert to summary format
        summaries = []
        for exp in experiments[:limit]:
            summary = ExperimentSummary(
                id=exp.id,
                name=exp.name,
                status=exp.status,
                feature_flag_key=exp.feature_flag_key,
                variants=[v.name for v in exp.variants],
                start_date=exp.start_date,
                end_date=exp.end_date,
                created_by=exp.created_by,
                winner=exp.winner,
                confidence=exp.confidence,
            )
            summaries.append(summary)

        logger.info(
            "A/B test experiments listed via API",
            count=len(summaries),
            user=current_user.username,
        )

        return StandardResponse(
            success=True,
            data=summaries,
            message=f"Retrieved {len(summaries)} experiments",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        logger.error("Failed to list experiments", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to list experiments: {str(e)}"
        )


@router.delete("/{experiment_id}", response_model=StandardResponse[dict])
async def delete_experiment(
    request: Request,
    experiment_id: str,
    current_user: User = Depends(get_current_user),
    engine: AsyncABTestingEngine = Depends(get_ab_testing_engine),
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[dict]:
    """Delete an experiment (archive)."""
    try:
        experiment = await engine.get_experiment(
            experiment_uuid=experiment_id, session=db
        )

        if not experiment:
            raise HTTPException(status_code=404, detail="Experiment not found")

        # Archive experiment instead of deleting
        experiment.status = ExperimentStatus.ARCHIVED
        experiment.updated_at = datetime.now()

        logger.info(
            "A/B test experiment archived via API",
            experiment_id=experiment_id,
            user=current_user.username,
        )

        return StandardResponse(
            success=True,
            data={
                "experiment_id": experiment_id,
                "status": "archived",
                "archived_at": experiment.updated_at.isoformat(),
            },
            message="Experiment archived successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Failed to archive experiment", experiment_id=experiment_id, error=str(e)
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to archive experiment: {str(e)}"
        )
