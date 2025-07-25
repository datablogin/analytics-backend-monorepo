"""Report management routes."""

import time
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from libs.analytics_core.auth import get_current_user
from libs.analytics_core.database import get_db_session
from libs.api_common.response_models import APIMetadata, StandardResponse
from libs.observability import trace_function

from ..models import (
    Report,
    ReportCreateRequest,
    ReportExecution,
    ReportExecutionRequest,
    ReportExecutionResponse,
    ReportResponse,
    ReportStatus,
    ReportUpdateRequest,
)
from ..tasks import export_report, generate_report

router = APIRouter()


@router.post(
    "/",
    response_model=StandardResponse[ReportResponse],
    summary="Create Report",
    description="Create a new report configuration",
)
@trace_function("create_report")
async def create_report(
    request: ReportCreateRequest,
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[ReportResponse]:
    """Create a new report."""

    try:
        # Create new report
        report = Report(
            name=request.name,
            description=request.description,
            report_type=request.report_type,
            config=request.config,
            filters=request.filters,
            frequency=request.frequency,
            next_run=request.next_run,
            created_by=current_user.get("user_id", "unknown"),
        )

        db.add(report)
        await db.commit()
        await db.refresh(report)

        return StandardResponse(
            success=True,
            data=ReportResponse.model_validate(report),
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to create report: {str(e)}"
        )


@router.get(
    "/",
    response_model=StandardResponse[list[ReportResponse]],
    summary="List Reports",
    description="Get all reports for the current user",
)
@trace_function("list_reports")
async def list_reports(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    report_type: str | None = Query(None, description="Filter by report type"),
    status: ReportStatus | None = Query(None, description="Filter by status"),
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[list[ReportResponse]]:
    """Get all reports for the current user."""

    try:
        # Build query
        query = select(Report).where(
            Report.created_by == current_user.get("user_id", "unknown")
        )

        if report_type is not None:
            query = query.where(Report.report_type == report_type)  # type: ignore[arg-type]
        if status is not None:
            query = query.where(Report.status == status)  # type: ignore[arg-type]

        query = query.offset(skip).limit(limit).order_by(Report.created_at.desc())

        result = await db.execute(query)
        reports = result.scalars().all()

        return StandardResponse(
            success=True,
            data=[ReportResponse.model_validate(report) for report in reports],
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {str(e)}")


@router.get(
    "/{report_id}",
    response_model=StandardResponse[ReportResponse],
    summary="Get Report",
    description="Get a specific report by ID",
)
@trace_function("get_report")
async def get_report(
    report_id: str,
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[ReportResponse]:
    """Get a specific report by ID."""

    try:
        query = select(Report).where(
            Report.id == report_id,
            Report.created_by == current_user.get("user_id", "unknown"),
        )
        result = await db.execute(query)
        report = result.scalar_one_or_none()

        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        return StandardResponse(
            success=True,
            data=ReportResponse.model_validate(report),
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get report: {str(e)}")


@router.put(
    "/{report_id}",
    response_model=StandardResponse[ReportResponse],
    summary="Update Report",
    description="Update an existing report",
)
@trace_function("update_report")
async def update_report(
    report_id: str,
    request: ReportUpdateRequest,
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[ReportResponse]:
    """Update an existing report."""

    try:
        query = select(Report).where(
            Report.id == report_id,
            Report.created_by == current_user.get("user_id", "unknown"),
        )
        result = await db.execute(query)
        report = result.scalar_one_or_none()

        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        # Update fields
        if request.name is not None:
            report.name = request.name
        if request.description is not None:
            report.description = request.description
        if request.config is not None:
            report.config = request.config
        if request.filters is not None:
            report.filters = request.filters
        if request.frequency is not None:
            report.frequency = request.frequency
        if request.next_run is not None:
            report.next_run = request.next_run

        await db.commit()
        await db.refresh(report)

        return StandardResponse(
            success=True,
            data=ReportResponse.model_validate(report),
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to update report: {str(e)}"
        )


@router.delete(
    "/{report_id}",
    response_model=StandardResponse[None],
    summary="Delete Report",
    description="Delete a report",
)
@trace_function("delete_report")
async def delete_report(
    report_id: str,
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[None]:
    """Delete a report."""

    try:
        query = select(Report).where(
            Report.id == report_id,
            Report.created_by == current_user.get("user_id", "unknown"),
        )
        result = await db.execute(query)
        report = result.scalar_one_or_none()

        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        await db.delete(report)
        await db.commit()

        return StandardResponse(
            success=True,
            data=None,
            message="Report deleted successfully",
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to delete report: {str(e)}"
        )


@router.post(
    "/{report_id}/execute",
    response_model=StandardResponse[ReportExecutionResponse],
    summary="Execute Report",
    description="Execute a report and optionally export to specified format",
)
@trace_function("execute_report")
async def execute_report(
    report_id: str,
    request: ReportExecutionRequest,
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[ReportExecutionResponse]:
    """Execute a report."""

    try:
        # Get report
        query = select(Report).where(
            Report.id == report_id,
            Report.created_by == current_user.get("user_id", "unknown"),
        )
        result = await db.execute(query)
        report = result.scalar_one_or_none()

        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        # Create execution record
        execution = ReportExecution(
            report_id=report_id,
            format=request.format,
            task_id=None,  # Will be set after task creation
        )

        db.add(execution)
        await db.commit()
        await db.refresh(execution)

        if request.async_execution:
            # Generate report asynchronously
            task = generate_report.delay(
                str(report_id),
                {
                    "report_type": report.report_type.value,
                    "config": report.config,
                    "filters": report.filters,
                },
            )

            # Update execution with task ID
            execution.task_id = task.id
            await db.commit()

            # Update report status
            report.status = ReportStatus.IN_PROGRESS
            await db.commit()

        else:
            # Generate report synchronously (not recommended for large reports)
            try:
                report_result = generate_report(
                    str(report_id),
                    {
                        "report_type": report.report_type.value,
                        "config": report.config,
                        "filters": report.filters,
                    },
                )

                # Export if requested
                if request.format:
                    filename = f"report_{report_id}_{int(time.time())}"
                    export_result = export_report(
                        report_result["data"], request.format.value, filename
                    )

                    execution.file_path = export_result["file_path"]
                    execution.file_size_bytes = str(export_result["file_size_bytes"])

                execution.status = ReportStatus.COMPLETED
                execution.completed_at = time.time()
                execution.processing_time_ms = str(report_result["processing_time_ms"])

                report.status = ReportStatus.COMPLETED
                report.processing_time_ms = str(report_result["processing_time_ms"])

                await db.commit()

            except Exception as e:
                execution.status = ReportStatus.FAILED
                execution.error_message = str(e)
                report.status = ReportStatus.FAILED
                report.error_message = str(e)
                await db.commit()
                raise HTTPException(
                    status_code=500, detail=f"Report execution failed: {str(e)}"
                )

        return StandardResponse(
            success=True,
            data=ReportExecutionResponse.model_validate(execution),
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to execute report: {str(e)}"
        )


@router.get(
    "/{report_id}/executions",
    response_model=StandardResponse[list[ReportExecutionResponse]],
    summary="List Report Executions",
    description="Get execution history for a report",
)
@trace_function("list_report_executions")
async def list_report_executions(
    report_id: str,
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(50, ge=1, le=100, description="Number of records to return"),
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[list[ReportExecutionResponse]]:
    """Get execution history for a report."""

    try:
        # Verify report ownership
        report_query = select(Report).where(
            Report.id == report_id,
            Report.created_by == current_user.get("user_id", "unknown"),
        )
        report_result = await db.execute(report_query)
        report = report_result.scalar_one_or_none()

        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        # Get executions
        query = (
            select(ReportExecution)
            .where(ReportExecution.report_id == report_id)
            .offset(skip)
            .limit(limit)
            .order_by(ReportExecution.started_at.desc())
        )

        result = await db.execute(query)
        executions = result.scalars().all()

        return StandardResponse(
            success=True,
            data=[
                ReportExecutionResponse.model_validate(execution)
                for execution in executions
            ],
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to list executions: {str(e)}"
        )
