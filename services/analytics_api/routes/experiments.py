"""ML Experiment tracking API endpoints."""

from typing import Any

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
)
from sqlalchemy.ext.asyncio import AsyncSession

from libs.analytics_core.auth import get_current_user
from libs.analytics_core.database import get_db_session
from libs.analytics_core.models import User
from libs.api_common.response_models import APIMetadata, StandardResponse
from libs.ml_models import (
    ExperimentCreate,
    ExperimentResponse,
    ExperimentTracker,
    MetricCreate,
    ParamCreate,
    RunCreate,
    RunResponse,
)
from libs.ml_models.artifact_storage import ArtifactInfo, ArtifactManager

router = APIRouter(prefix="/experiments", tags=["ML Experiments"])


@router.post("", response_model=StandardResponse[ExperimentResponse])
async def create_experiment(
    request: Request,
    experiment_data: ExperimentCreate,
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[ExperimentResponse]:
    """Create a new ML experiment."""
    tracker = ExperimentTracker(db)

    try:
        experiment = await tracker.create_experiment(
            experiment_data, owner_id=current_user.id
        )

        return StandardResponse(
            success=True,
            data=experiment,
            message="Experiment created successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("", response_model=StandardResponse[list[ExperimentResponse]])
async def list_experiments(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[list[ExperimentResponse]]:
    """List ML experiments with pagination."""
    tracker = ExperimentTracker(db)

    experiments = await tracker.list_experiments(limit=limit, offset=offset)

    return StandardResponse(
        success=True,
        data=experiments,
        message=f"Retrieved {len(experiments)} experiments",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@router.get("/{experiment_id}", response_model=StandardResponse[ExperimentResponse])
async def get_experiment(
    request: Request,
    experiment_id: int,
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[ExperimentResponse]:
    """Get a specific experiment by ID."""
    tracker = ExperimentTracker(db)

    experiment = await tracker.get_experiment(experiment_id)

    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")

    return StandardResponse(
        success=True,
        data=experiment,
        message="Experiment retrieved successfully",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@router.post("/{experiment_id}/runs", response_model=StandardResponse[RunResponse])
async def create_run(
    request: Request,
    experiment_id: int,
    run_data: RunCreate,
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[RunResponse]:
    """Create a new experiment run."""
    tracker = ExperimentTracker(db)

    try:
        run = await tracker.create_run(experiment_id, run_data)

        return StandardResponse(
            success=True,
            data=run,
            message="Experiment run created successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{experiment_id}/runs", response_model=StandardResponse[list[RunResponse]])
async def list_runs(
    request: Request,
    experiment_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[list[RunResponse]]:
    """List runs for an experiment."""
    tracker = ExperimentTracker(db)

    runs = await tracker.list_runs(experiment_id, limit=limit, offset=offset)

    return StandardResponse(
        success=True,
        data=runs,
        message=f"Retrieved {len(runs)} runs for experiment {experiment_id}",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@router.post("/runs/{run_uuid}/metrics", response_model=StandardResponse[dict])
async def log_metric(
    request: Request,
    run_uuid: str,
    metric: MetricCreate,
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Log a metric for an experiment run."""
    tracker = ExperimentTracker(db)

    try:
        await tracker.log_metric(run_uuid, metric)

        return StandardResponse(
            success=True,
            data={"message": f"Metric '{metric.key}' logged successfully"},
            message="Metric logged successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/runs/{run_uuid}/params", response_model=StandardResponse[dict])
async def log_param(
    request: Request,
    run_uuid: str,
    param: ParamCreate,
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Log a parameter for an experiment run."""
    tracker = ExperimentTracker(db)

    try:
        await tracker.log_param(run_uuid, param)

        return StandardResponse(
            success=True,
            data={"message": f"Parameter '{param.key}' logged successfully"},
            message="Parameter logged successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/runs/{run_uuid}/finish", response_model=StandardResponse[dict])
async def finish_run(
    request: Request,
    run_uuid: str,
    status: str = "FINISHED",
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Finish an experiment run."""
    tracker = ExperimentTracker(db)

    try:
        await tracker.finish_run(run_uuid, status)

        return StandardResponse(
            success=True,
            data={"message": f"Run {run_uuid} finished with status: {status}"},
            message="Run finished successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/runs/{run_uuid}/metrics", response_model=StandardResponse[list[dict[str, Any]]]
)
async def get_run_metrics(
    request: Request,
    run_uuid: str,
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[list[dict[str, Any]]]:
    """Get all metrics for an experiment run."""
    tracker = ExperimentTracker(db)

    metrics = await tracker.get_run_metrics(run_uuid)

    return StandardResponse(
        success=True,
        data=metrics,
        message=f"Retrieved {len(metrics)} metrics for run {run_uuid}",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@router.get("/runs/{run_uuid}/params", response_model=StandardResponse[dict[str, str]])
async def get_run_params(
    request: Request,
    run_uuid: str,
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict[str, str]]:
    """Get all parameters for an experiment run."""
    tracker = ExperimentTracker(db)

    params = await tracker.get_run_params(run_uuid)

    return StandardResponse(
        success=True,
        data=params,
        message=f"Retrieved {len(params)} parameters for run {run_uuid}",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@router.post("/compare", response_model=StandardResponse[dict[str, Any]])
async def compare_runs(
    request: Request,
    run_uuids: list[str],
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict[str, Any]]:
    """Compare multiple experiment runs."""
    if len(run_uuids) < 2:
        raise HTTPException(
            status_code=400, detail="At least 2 runs are required for comparison"
        )

    tracker = ExperimentTracker(db)

    comparison = await tracker.compare_runs(run_uuids)

    return StandardResponse(
        success=True,
        data=comparison,
        message=f"Comparison of {len(run_uuids)} runs completed",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@router.post("/runs/{run_uuid}/artifacts", response_model=StandardResponse[dict])
async def upload_artifact(
    request: Request,
    run_uuid: str,
    file: UploadFile = File(...),
    artifact_path: str | None = None,
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Upload an artifact for an experiment run."""
    artifact_manager = ArtifactManager()

    try:
        # Read file content
        content = await file.read()

        # Store the artifact
        metadata = await artifact_manager.store_artifact(
            run_uuid=run_uuid,
            artifact_name=file.filename or "uploaded_file",
            file_content=content,
            content_type=file.content_type or "application/octet-stream",
            artifact_path=artifact_path,
        )

        return StandardResponse(
            success=True,
            data={
                "name": metadata.name,
                "size": metadata.size,
                "checksum": metadata.checksum,
                "upload_time": metadata.upload_time.isoformat(),
            },
            message=f"Artifact '{metadata.name}' uploaded successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/runs/{run_uuid}/artifacts", response_model=StandardResponse[list[ArtifactInfo]]
)
async def list_artifacts(
    request: Request,
    run_uuid: str,
    current_user: User = Depends(get_current_user),
) -> StandardResponse[list[ArtifactInfo]]:
    """List all artifacts for an experiment run."""
    artifact_manager = ArtifactManager()

    artifacts = await artifact_manager.list_artifacts(run_uuid)

    return StandardResponse(
        success=True,
        data=artifacts,
        message=f"Retrieved {len(artifacts)} artifacts for run {run_uuid}",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@router.get("/runs/{run_uuid}/artifacts/{artifact_name}")
async def download_artifact(
    run_uuid: str,
    artifact_name: str,
    current_user: User = Depends(get_current_user),
) -> Response:
    """Download an artifact from an experiment run."""
    artifact_manager = ArtifactManager()

    try:
        result = await artifact_manager.get_artifact(run_uuid, artifact_name)
        if not result:
            raise HTTPException(status_code=404, detail="Artifact not found")

        content, metadata = result

        return Response(
            content=content,
            media_type=metadata.content_type,
            headers={
                "Content-Disposition": f"attachment; filename={metadata.name}",
                "Content-Length": str(metadata.size),
                "X-Artifact-Checksum": metadata.checksum,
            },
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/runs/{run_uuid}/artifacts/{artifact_name}", response_model=StandardResponse[dict]
)
async def delete_artifact(
    request: Request,
    run_uuid: str,
    artifact_name: str,
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Delete an artifact from an experiment run."""
    artifact_manager = ArtifactManager()

    success = await artifact_manager.delete_artifact(run_uuid, artifact_name)

    if not success:
        raise HTTPException(status_code=404, detail="Artifact not found")

    return StandardResponse(
        success=True,
        data={"message": f"Artifact '{artifact_name}' deleted successfully"},
        message="Artifact deleted successfully",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )
