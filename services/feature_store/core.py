"""Core feature store service logic."""

import json
from datetime import datetime
from typing import Any

from libs.analytics_core.database import DatabaseManager
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import (
    Feature,
    FeatureDefinitionCreate,
    FeatureDefinitionResponse,
    FeatureDefinitionUpdate,
    FeatureDiscoveryResponse,
    FeatureServingResponse,
    FeatureStatus,
    FeatureType,
    FeatureValidationResult,
    FeatureValue,
    FeatureValueResponse,
    FeatureValueWrite,
)

# Security constants for JSON parsing
MAX_JSON_SIZE = 1024 * 1024  # 1MB limit for JSON strings
MAX_JSON_DEPTH = 20  # Maximum nesting depth


def _safe_json_loads(json_string: str, max_size: int = MAX_JSON_SIZE) -> Any:
    """Safely parse JSON with size and depth validation."""
    if len(json_string) > max_size:
        raise ValueError(
            f"JSON string too large: {len(json_string)} bytes (max: {max_size})"
        )

    try:
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {str(e)}")


def _validate_json_depth(
    obj: Any, current_depth: int = 0, max_depth: int = MAX_JSON_DEPTH
) -> None:
    """Validate JSON object depth to prevent deeply nested structures."""
    if current_depth > max_depth:
        raise ValueError(f"JSON nesting too deep: {current_depth} (max: {max_depth})")

    if isinstance(obj, dict):
        for value in obj.values():
            _validate_json_depth(value, current_depth + 1, max_depth)
    elif isinstance(obj, list):
        for item in obj:
            _validate_json_depth(item, current_depth + 1, max_depth)


class FeatureStoreService:
    """Core feature store service for managing features and values."""

    def __init__(self, db_manager: DatabaseManager) -> None:
        """Initialize the feature store service."""
        self.db_manager = db_manager

    async def create_feature(
        self, feature_data: FeatureDefinitionCreate, session: AsyncSession | None = None
    ) -> FeatureDefinitionResponse:
        """Create a new feature definition."""
        if session:
            return await self._create_feature_with_session(session, feature_data)

        async with self.db_manager.get_session() as session:  # type: ignore
            return await self._create_feature_with_session(session, feature_data)

    async def _create_feature_with_session(
        self, session: AsyncSession, feature_data: FeatureDefinitionCreate
    ) -> FeatureDefinitionResponse:
        """Internal method to create feature with existing session."""
        # Convert Pydantic model to dict and handle JSON fields
        feature_dict = feature_data.model_dump()

        # Convert JSON fields to strings
        if feature_dict.get("default_value") is not None:
            feature_dict["default_value"] = json.dumps(feature_dict["default_value"])
        if feature_dict.get("validation_rules") is not None:
            feature_dict["validation_rules"] = json.dumps(
                feature_dict["validation_rules"]
            )
        if feature_dict.get("tags") is not None:
            feature_dict["tags"] = json.dumps(feature_dict["tags"])

        feature = Feature(**feature_dict)
        session.add(feature)
        await session.commit()
        await session.refresh(feature)

        return await self._feature_to_response(feature)

    async def get_feature(
        self, feature_name: str, session: AsyncSession | None = None
    ) -> FeatureDefinitionResponse | None:
        """Get a feature definition by name."""
        if session:
            return await self._get_feature_with_session(session, feature_name)

        async with self.db_manager.get_session() as session:  # type: ignore
            return await self._get_feature_with_session(session, feature_name)

    async def _get_feature_with_session(
        self, session: AsyncSession, feature_name: str
    ) -> FeatureDefinitionResponse | None:
        """Internal method to get feature with existing session."""
        stmt = select(Feature).where(Feature.name == feature_name)
        result = await session.execute(stmt)
        feature = result.scalar_one_or_none()

        if feature is None:
            return None

        return await self._feature_to_response(feature)

    async def list_features(
        self,
        feature_group: str | None = None,
        status: FeatureStatus | None = None,
        limit: int = 100,
        offset: int = 0,
        session: AsyncSession | None = None,
    ) -> list[FeatureDefinitionResponse]:
        """List features with optional filtering."""
        if session:
            return await self._list_features_with_session(
                session, feature_group, status, limit, offset
            )

        async with self.db_manager.get_session() as session:  # type: ignore
            return await self._list_features_with_session(
                session, feature_group, status, limit, offset
            )

    async def _list_features_with_session(
        self,
        session: AsyncSession,
        feature_group: str | None = None,
        status: FeatureStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[FeatureDefinitionResponse]:
        """Internal method to list features with existing session."""
        stmt = select(Feature)

        # Apply filters
        if feature_group:
            stmt = stmt.where(Feature.feature_group == feature_group)
        if status:
            stmt = stmt.where(Feature.status == status)

        stmt = stmt.offset(offset).limit(limit).order_by(Feature.created_at.desc())

        result = await session.execute(stmt)
        features = result.scalars().all()

        return [await self._feature_to_response(feature) for feature in features]

    async def update_feature(
        self, feature_name: str, update_data: FeatureDefinitionUpdate
    ) -> FeatureDefinitionResponse | None:
        """Update a feature definition."""
        async with self.db_manager.get_session() as session:  # type: ignore
            stmt = select(Feature).where(Feature.name == feature_name)
            result = await session.execute(stmt)
            feature = result.scalar_one_or_none()

            if feature is None:
                return None

            # Update fields
            update_dict = update_data.model_dump(exclude_unset=True)

            # Convert JSON fields to strings
            if (
                "default_value" in update_dict
                and update_dict["default_value"] is not None
            ):
                update_dict["default_value"] = json.dumps(update_dict["default_value"])
            if (
                "validation_rules" in update_dict
                and update_dict["validation_rules"] is not None
            ):
                update_dict["validation_rules"] = json.dumps(
                    update_dict["validation_rules"]
                )
            if "tags" in update_dict and update_dict["tags"] is not None:
                update_dict["tags"] = json.dumps(update_dict["tags"])

            for field, value in update_dict.items():
                setattr(feature, field, value)

            await session.commit()
            await session.refresh(feature)

            return await self._feature_to_response(feature)

    async def delete_feature(self, feature_name: str) -> bool:
        """Delete a feature definition."""
        async with self.db_manager.get_session() as session:  # type: ignore
            stmt = select(Feature).where(Feature.name == feature_name)
            result = await session.execute(stmt)
            feature = result.scalar_one_or_none()

            if feature is None:
                return False

            await session.delete(feature)
            await session.commit()
            return True

    async def write_feature_value(
        self, value_data: FeatureValueWrite
    ) -> FeatureValueResponse:
        """Write a single feature value."""
        async with self.db_manager.get_session() as session:  # type: ignore
            # Validate feature exists
            feature = await self._get_feature_by_name(session, value_data.feature_name)
            if feature is None:
                raise ValueError(f"Feature '{value_data.feature_name}' does not exist")

            # Validate value
            validation_result = await self._validate_feature_value(
                feature, value_data.value
            )
            if not validation_result.is_valid:
                raise ValueError(
                    f"Invalid value: {', '.join(validation_result.errors)}"
                )

            # Create feature value
            timestamp = value_data.timestamp or datetime.utcnow()
            feature_value = FeatureValue(
                feature_name=value_data.feature_name,
                entity_id=value_data.entity_id,
                value=json.dumps(value_data.value),
                timestamp=timestamp,
            )

            session.add(feature_value)
            await session.commit()
            await session.refresh(feature_value)

            return await self._feature_value_to_response(feature_value)

    async def write_feature_values_batch(
        self, values: list[FeatureValueWrite]
    ) -> list[FeatureValueResponse]:
        """Write multiple feature values in a batch."""
        async with self.db_manager.get_session() as session:  # type: ignore
            responses = []

            for value_data in values:
                # Validate feature exists
                feature = await self._get_feature_by_name(
                    session, value_data.feature_name
                )
                if feature is None:
                    raise ValueError(
                        f"Feature '{value_data.feature_name}' does not exist"
                    )

                # Validate value
                validation_result = await self._validate_feature_value(
                    feature, value_data.value
                )
                if not validation_result.is_valid:
                    raise ValueError(
                        f"Invalid value for {value_data.feature_name}: {', '.join(validation_result.errors)}"
                    )

                # Create feature value
                timestamp = value_data.timestamp or datetime.utcnow()
                feature_value = FeatureValue(
                    feature_name=value_data.feature_name,
                    entity_id=value_data.entity_id,
                    value=json.dumps(value_data.value),
                    timestamp=timestamp,
                )

                session.add(feature_value)
                responses.append(feature_value)

            await session.commit()

            # Bulk refresh using select query for better performance
            feature_value_ids = [fv.id for fv in responses]
            stmt = select(FeatureValue).where(FeatureValue.id.in_(feature_value_ids))
            result = await session.execute(stmt)
            refreshed_values = result.scalars().all()

            # Convert to responses
            result_responses = []
            for feature_value in refreshed_values:
                result_responses.append(
                    await self._feature_value_to_response(feature_value)
                )

            return result_responses

    async def read_feature_values(
        self,
        feature_names: list[str],
        entity_ids: list[str],
        timestamp: datetime | None = None,
    ) -> list[FeatureValueResponse]:
        """Read feature values for given features and entities."""
        async with self.db_manager.get_session() as session:  # type: ignore
            stmt = select(FeatureValue).where(
                and_(
                    FeatureValue.feature_name.in_(feature_names),
                    FeatureValue.entity_id.in_(entity_ids),
                )
            )

            if timestamp:
                stmt = stmt.where(FeatureValue.timestamp <= timestamp)

            # Get latest values for each feature-entity combination
            stmt = stmt.order_by(FeatureValue.timestamp.desc())

            result = await session.execute(stmt)
            feature_values = result.scalars().all()

            # Group by feature_name and entity_id to get latest values
            latest_values = {}
            for fv in feature_values:
                key = (fv.feature_name, fv.entity_id)
                if key not in latest_values:
                    latest_values[key] = fv

            return [
                await self._feature_value_to_response(fv)
                for fv in latest_values.values()
            ]

    async def serve_features(
        self,
        feature_names: list[str],
        entity_id: str,
        timestamp: datetime | None = None,
    ) -> FeatureServingResponse:
        """Serve features for online inference."""
        feature_values = await self.read_feature_values(
            feature_names=feature_names,
            entity_ids=[entity_id],
            timestamp=timestamp,
        )

        # Convert to serving format
        features = {}
        for fv in feature_values:
            if fv.entity_id == entity_id:
                features[fv.feature_name] = fv.value

        # Fill missing features with default values
        async with self.db_manager.get_session() as session:  # type: ignore
            for feature_name in feature_names:
                if feature_name not in features:
                    feature = await self._get_feature_by_name(session, feature_name)
                    if feature and feature.default_value:
                        try:
                            parsed_value = _safe_json_loads(feature.default_value)
                            _validate_json_depth(parsed_value)
                            features[feature_name] = parsed_value
                        except ValueError:
                            # Fallback to None if JSON parsing fails
                            features[feature_name] = None
                    else:
                        features[feature_name] = None

        return FeatureServingResponse(
            entity_id=entity_id,
            features=features,
            timestamp=timestamp or datetime.utcnow(),
        )

    async def discover_features(self) -> FeatureDiscoveryResponse:
        """Discover available features and statistics."""
        async with self.db_manager.get_session() as session:  # type: ignore
            # Get all features
            stmt = select(Feature)
            result = await session.execute(stmt)
            features = result.scalars().all()

            # Calculate statistics
            feature_groups = list({f.feature_group for f in features})
            total_features = len(features)

            features_by_type = {}
            for feature_type in FeatureType:
                features_by_type[feature_type] = sum(
                    1 for f in features if f.feature_type == feature_type
                )

            features_by_status = {}
            for status in FeatureStatus:
                features_by_status[status] = sum(
                    1 for f in features if f.status == status
                )

            return FeatureDiscoveryResponse(
                feature_groups=feature_groups,
                total_features=total_features,
                features_by_type=features_by_type,
                features_by_status=features_by_status,
            )

    async def _get_feature_by_name(
        self, session: AsyncSession, feature_name: str
    ) -> Feature | None:
        """Helper method to get feature by name."""
        stmt = select(Feature).where(Feature.name == feature_name)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def _validate_feature_value(
        self, feature: Feature, value: Any
    ) -> FeatureValidationResult:
        """Validate a feature value against its definition."""
        errors = []
        warnings = []

        # Type validation
        try:
            if feature.feature_type == FeatureType.STRING and not isinstance(
                value, str
            ):
                errors.append(f"Expected string, got {type(value).__name__}")
            elif feature.feature_type == FeatureType.INTEGER and not isinstance(
                value, int
            ):
                errors.append(f"Expected integer, got {type(value).__name__}")
            elif feature.feature_type == FeatureType.FLOAT and not isinstance(
                value, int | float
            ):
                errors.append(f"Expected float, got {type(value).__name__}")
            elif feature.feature_type == FeatureType.BOOLEAN and not isinstance(
                value, bool
            ):
                errors.append(f"Expected boolean, got {type(value).__name__}")
            elif feature.feature_type == FeatureType.TIMESTAMP:
                if not isinstance(value, datetime | str):
                    errors.append(
                        f"Expected datetime or ISO string, got {type(value).__name__}"
                    )
        except Exception as e:
            errors.append(f"Type validation error: {str(e)}")

        # Custom validation rules
        if feature.validation_rules:
            try:
                rules = _safe_json_loads(feature.validation_rules)
                _validate_json_depth(rules)
                # Add custom validation logic here based on rules
                # For now, just check basic constraints
                if "min_value" in rules and isinstance(value, int | float):
                    if value < rules["min_value"]:
                        errors.append(
                            f"Value {value} is below minimum {rules['min_value']}"
                        )
                if "max_value" in rules and isinstance(value, int | float):
                    if value > rules["max_value"]:
                        errors.append(
                            f"Value {value} is above maximum {rules['max_value']}"
                        )
                if "max_length" in rules and isinstance(value, str):
                    if len(value) > rules["max_length"]:
                        errors.append(
                            f"String length {len(value)} exceeds maximum {rules['max_length']}"
                        )
            except Exception as e:
                warnings.append(f"Could not apply validation rules: {str(e)}")

        return FeatureValidationResult(
            feature_name=feature.name,
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    async def _feature_to_response(self, feature: Feature) -> FeatureDefinitionResponse:
        """Convert Feature model to response model."""
        # Parse JSON fields with security validation
        default_value = None
        if feature.default_value:
            try:
                default_value = _safe_json_loads(feature.default_value)
                _validate_json_depth(default_value)
            except ValueError:
                default_value = feature.default_value

        validation_rules = None
        if feature.validation_rules:
            try:
                validation_rules = _safe_json_loads(feature.validation_rules)
                _validate_json_depth(validation_rules)
            except ValueError:
                validation_rules = {}

        tags = None
        if feature.tags:
            try:
                tags = _safe_json_loads(feature.tags)
                _validate_json_depth(tags)
            except ValueError:
                tags = []

        return FeatureDefinitionResponse(
            id=feature.id,
            name=feature.name,
            feature_group=feature.feature_group,
            description=feature.description,
            feature_type=feature.feature_type,
            status=feature.status,
            default_value=default_value,
            validation_rules=validation_rules,
            tags=tags,
            owner=feature.owner,
            version=feature.version,
            created_at=feature.created_at,
            updated_at=feature.updated_at,
        )

    async def _feature_value_to_response(
        self, feature_value: FeatureValue
    ) -> FeatureValueResponse:
        """Convert FeatureValue model to response model."""
        # Parse JSON value with security validation
        value = None
        try:
            value = _safe_json_loads(feature_value.value)
            _validate_json_depth(value)
        except ValueError:
            value = feature_value.value

        return FeatureValueResponse(
            id=feature_value.id,
            feature_name=feature_value.feature_name,
            entity_id=feature_value.entity_id,
            value=value,
            timestamp=feature_value.timestamp,
            version=feature_value.version,
            created_at=feature_value.created_at,
        )
