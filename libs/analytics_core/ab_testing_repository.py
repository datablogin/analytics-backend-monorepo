"""Repository for A/B Testing database operations."""

from datetime import datetime
from typing import Any

import structlog
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .models import ABTestAssignment, ABTestEvent, ABTestExperiment

logger = structlog.get_logger(__name__)


class ABTestRepository:
    """Repository for A/B testing database operations."""

    def __init__(self, session: AsyncSession):
        """Initialize repository with database session."""
        self.session = session
        self.logger = logger.bind(component="ab_test_repository")

    async def create_experiment(
        self, experiment_data: dict[str, Any]
    ) -> ABTestExperiment:
        """Create a new A/B test experiment."""
        try:
            experiment = ABTestExperiment(**experiment_data)
            self.session.add(experiment)
            await self.session.commit()
            await self.session.refresh(experiment)

            self.logger.info(
                "A/B test experiment created",
                experiment_id=experiment.id,
                experiment_uuid=experiment.experiment_uuid,
                name=experiment.name,
            )

            return experiment

        except Exception as error:
            await self.session.rollback()
            self.logger.error("Failed to create experiment", error=str(error))
            raise

    async def get_experiment_by_uuid(
        self, experiment_uuid: str
    ) -> ABTestExperiment | None:
        """Get experiment by UUID."""
        try:
            result = await self.session.execute(
                select(ABTestExperiment)
                .where(ABTestExperiment.experiment_uuid == experiment_uuid)
                .options(selectinload(ABTestExperiment.assignments))
                .options(selectinload(ABTestExperiment.events))
            )
            return result.scalar_one_or_none()

        except Exception as error:
            self.logger.error(
                "Failed to get experiment",
                experiment_uuid=experiment_uuid,
                error=str(error),
            )
            raise

    async def get_experiment_by_id(self, experiment_id: int) -> ABTestExperiment | None:
        """Get experiment by database ID."""
        try:
            result = await self.session.execute(
                select(ABTestExperiment)
                .where(ABTestExperiment.id == experiment_id)
                .options(selectinload(ABTestExperiment.assignments))
                .options(selectinload(ABTestExperiment.events))
            )
            return result.scalar_one_or_none()

        except Exception as error:
            self.logger.error(
                "Failed to get experiment",
                experiment_id=experiment_id,
                error=str(error),
            )
            raise

    async def update_experiment(
        self, experiment_id: int, updates: dict[str, Any]
    ) -> ABTestExperiment | None:
        """Update experiment with new data."""
        try:
            experiment = await self.get_experiment_by_id(experiment_id)
            if not experiment:
                return None

            for key, value in updates.items():
                if hasattr(experiment, key):
                    setattr(experiment, key, value)

            experiment.updated_at = datetime.utcnow()
            await self.session.commit()
            await self.session.refresh(experiment)

            self.logger.info(
                "A/B test experiment updated",
                experiment_id=experiment_id,
                updates=list(updates.keys()),
            )

            return experiment

        except Exception as error:
            await self.session.rollback()
            self.logger.error(
                "Failed to update experiment",
                experiment_id=experiment_id,
                error=str(error),
            )
            raise

    async def list_experiments(
        self, status: str | None = None, created_by: int | None = None, limit: int = 100
    ) -> list[ABTestExperiment]:
        """List experiments with optional filtering."""
        try:
            query = select(ABTestExperiment).options(
                selectinload(ABTestExperiment.creator)
            )

            if status:
                query = query.where(ABTestExperiment.status == status)
            if created_by:
                query = query.where(ABTestExperiment.created_by == created_by)

            query = query.limit(limit).order_by(ABTestExperiment.created_at.desc())

            result = await self.session.execute(query)
            return list(result.scalars().all())

        except Exception as error:
            self.logger.error("Failed to list experiments", error=str(error))
            raise

    async def create_assignment(
        self, assignment_data: dict[str, Any]
    ) -> ABTestAssignment:
        """Create a new user assignment."""
        try:
            assignment = ABTestAssignment(**assignment_data)
            self.session.add(assignment)
            await self.session.commit()
            await self.session.refresh(assignment)

            self.logger.debug(
                "User assignment created",
                experiment_id=assignment.experiment_id,
                user_id=assignment.user_id,
                variant=assignment.variant,
            )

            return assignment

        except Exception as error:
            await self.session.rollback()
            self.logger.error("Failed to create assignment", error=str(error))
            raise

    async def get_user_assignment(
        self, experiment_id: int, user_id: str
    ) -> ABTestAssignment | None:
        """Get existing user assignment for experiment."""
        try:
            result = await self.session.execute(
                select(ABTestAssignment).where(
                    and_(
                        ABTestAssignment.experiment_id == experiment_id,
                        ABTestAssignment.user_id == user_id,
                    )
                )
            )
            return result.scalar_one_or_none()

        except Exception as error:
            self.logger.error(
                "Failed to get user assignment",
                experiment_id=experiment_id,
                user_id=user_id,
                error=str(error),
            )
            raise

    async def create_event(self, event_data: dict[str, Any]) -> ABTestEvent:
        """Create a new event."""
        try:
            event = ABTestEvent(**event_data)
            self.session.add(event)
            await self.session.commit()
            await self.session.refresh(event)

            self.logger.debug(
                "Event created",
                experiment_id=event.experiment_id,
                user_id=event.user_id,
                event_type=event.event_type,
                variant=event.variant,
            )

            return event

        except Exception as error:
            await self.session.rollback()
            self.logger.error("Failed to create event", error=str(error))
            raise

    async def get_experiment_events(
        self, experiment_id: int, event_type: str | None = None
    ) -> list[ABTestEvent]:
        """Get all events for an experiment."""
        try:
            query = select(ABTestEvent).where(
                ABTestEvent.experiment_id == experiment_id
            )

            if event_type:
                query = query.where(ABTestEvent.event_type == event_type)

            query = query.order_by(ABTestEvent.timestamp.asc())

            result = await self.session.execute(query)
            return list(result.scalars().all())

        except Exception as error:
            self.logger.error(
                "Failed to get experiment events",
                experiment_id=experiment_id,
                error=str(error),
            )
            raise

    async def get_experiment_assignments(
        self, experiment_id: int
    ) -> list[ABTestAssignment]:
        """Get all assignments for an experiment."""
        try:
            result = await self.session.execute(
                select(ABTestAssignment)
                .where(ABTestAssignment.experiment_id == experiment_id)
                .order_by(ABTestAssignment.assigned_at.asc())
            )
            return list(result.scalars().all())

        except Exception as error:
            self.logger.error(
                "Failed to get experiment assignments",
                experiment_id=experiment_id,
                error=str(error),
            )
            raise
