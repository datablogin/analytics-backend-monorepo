"""Tests for feature store models."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from ..models import (
    FeatureDefinitionCreate,
    FeatureDefinitionUpdate,
    FeatureDiscoveryResponse,
    FeatureServingResponse,
    FeatureStatus,
    FeatureType,
    FeatureValidationResult,
    FeatureValueBatchWrite,
    FeatureValueRead,
    FeatureValueWrite,
)


class TestFeatureDefinitionCreate:
    """Tests for FeatureDefinitionCreate model."""

    def test_valid_feature_definition(self):
        """Test creating a valid feature definition."""
        feature_data = FeatureDefinitionCreate(
            name="user_age",
            feature_group="user_demographics",
            description="User age in years",
            feature_type=FeatureType.INTEGER,
            default_value=25,
            validation_rules={"min_value": 0, "max_value": 120},
            tags=["demographic", "user"],
            owner="data_team",
        )

        assert feature_data.name == "user_age"
        assert feature_data.feature_group == "user_demographics"
        assert feature_data.feature_type == FeatureType.INTEGER
        assert feature_data.default_value == 25
        assert feature_data.validation_rules == {"min_value": 0, "max_value": 120}
        assert feature_data.tags == ["demographic", "user"]

    def test_required_fields_only(self):
        """Test creating feature with only required fields."""
        feature_data = FeatureDefinitionCreate(
            name="simple_feature",
            feature_group="test_group",
            feature_type=FeatureType.STRING,
        )

        assert feature_data.name == "simple_feature"
        assert feature_data.feature_group == "test_group"
        assert feature_data.feature_type == FeatureType.STRING
        assert feature_data.description is None
        assert feature_data.default_value is None

    def test_name_validation(self):
        """Test name field validation."""
        # Empty name should fail
        with pytest.raises(ValidationError):
            FeatureDefinitionCreate(
                name="",
                feature_group="test_group",
                feature_type=FeatureType.STRING,
            )

        # Too long name should fail
        with pytest.raises(ValidationError):
            FeatureDefinitionCreate(
                name="a" * 256,  # Exceeds 255 char limit
                feature_group="test_group",
                feature_type=FeatureType.STRING,
            )

    def test_feature_type_validation(self):
        """Test feature type validation."""
        # Valid feature types
        for feature_type in FeatureType:
            feature_data = FeatureDefinitionCreate(
                name=f"test_{feature_type.value}",
                feature_group="test_group",
                feature_type=feature_type,
            )
            assert feature_data.feature_type == feature_type


class TestFeatureDefinitionUpdate:
    """Tests for FeatureDefinitionUpdate model."""

    def test_partial_update(self):
        """Test partial update with only some fields."""
        update_data = FeatureDefinitionUpdate(
            description="Updated description",
            tags=["new_tag"],
        )

        assert update_data.description == "Updated description"
        assert update_data.tags == ["new_tag"]
        assert update_data.status is None
        assert update_data.owner is None

    def test_status_update(self):
        """Test status field update."""
        update_data = FeatureDefinitionUpdate(status=FeatureStatus.DEPRECATED)
        assert update_data.status == FeatureStatus.DEPRECATED


class TestFeatureValueWrite:
    """Tests for FeatureValueWrite model."""

    def test_valid_feature_value(self):
        """Test creating a valid feature value."""
        timestamp = datetime.utcnow()
        value_data = FeatureValueWrite(
            feature_name="user_age",
            entity_id="user_123",
            value=30,
            timestamp=timestamp,
        )

        assert value_data.feature_name == "user_age"
        assert value_data.entity_id == "user_123"
        assert value_data.value == 30
        assert value_data.timestamp == timestamp

    def test_default_timestamp(self):
        """Test that timestamp defaults to None."""
        value_data = FeatureValueWrite(
            feature_name="user_age",
            entity_id="user_123",
            value=30,
        )

        assert value_data.timestamp is None

    def test_various_value_types(self):
        """Test different value types."""
        # String value
        value_data = FeatureValueWrite(
            feature_name="user_name",
            entity_id="user_123",
            value="John Doe",
        )
        assert value_data.value == "John Doe"

        # Boolean value
        value_data = FeatureValueWrite(
            feature_name="is_premium",
            entity_id="user_123",
            value=True,
        )
        assert value_data.value is True

        # Complex value (dict)
        complex_value = {"nested": {"data": [1, 2, 3]}}
        value_data = FeatureValueWrite(
            feature_name="user_metadata",
            entity_id="user_123",
            value=complex_value,
        )
        assert value_data.value == complex_value


class TestFeatureValueBatchWrite:
    """Tests for FeatureValueBatchWrite model."""

    def test_valid_batch(self):
        """Test creating a valid batch write."""
        values = [
            FeatureValueWrite(
                feature_name="user_age",
                entity_id="user_123",
                value=30,
            ),
            FeatureValueWrite(
                feature_name="user_name",
                entity_id="user_123",
                value="John",
            ),
        ]

        batch_data = FeatureValueBatchWrite(values=values)
        assert len(batch_data.values) == 2
        assert batch_data.values[0].feature_name == "user_age"
        assert batch_data.values[1].feature_name == "user_name"

    def test_empty_batch_fails(self):
        """Test that empty batch fails validation."""
        with pytest.raises(ValidationError):
            FeatureValueBatchWrite(values=[])


class TestFeatureValueRead:
    """Tests for FeatureValueRead model."""

    def test_valid_read_request(self):
        """Test creating a valid read request."""
        timestamp = datetime.utcnow()
        read_data = FeatureValueRead(
            feature_names=["user_age", "user_name"],
            entity_ids=["user_123", "user_456"],
            timestamp=timestamp,
        )

        assert read_data.feature_names == ["user_age", "user_name"]
        assert read_data.entity_ids == ["user_123", "user_456"]
        assert read_data.timestamp == timestamp

    def test_empty_lists_fail(self):
        """Test that empty lists fail validation."""
        with pytest.raises(ValidationError):
            FeatureValueRead(
                feature_names=[],
                entity_ids=["user_123"],
            )

        with pytest.raises(ValidationError):
            FeatureValueRead(
                feature_names=["user_age"],
                entity_ids=[],
            )


class TestFeatureServingResponse:
    """Tests for FeatureServingResponse model."""

    def test_valid_serving_response(self):
        """Test creating a valid serving response."""
        timestamp = datetime.utcnow()
        features = {"user_age": 30, "user_name": "John", "is_premium": True}

        response = FeatureServingResponse(
            entity_id="user_123",
            features=features,
            timestamp=timestamp,
        )

        assert response.entity_id == "user_123"
        assert response.features == features
        assert response.timestamp == timestamp


class TestFeatureDiscoveryResponse:
    """Tests for FeatureDiscoveryResponse model."""

    def test_valid_discovery_response(self):
        """Test creating a valid discovery response."""
        response = FeatureDiscoveryResponse(
            feature_groups=["user_demographics", "user_behavior"],
            total_features=10,
            features_by_type={
                FeatureType.INTEGER: 5,
                FeatureType.STRING: 3,
                FeatureType.BOOLEAN: 2,
            },
            features_by_status={
                FeatureStatus.ACTIVE: 8,
                FeatureStatus.INACTIVE: 1,
                FeatureStatus.DEPRECATED: 1,
            },
        )

        assert len(response.feature_groups) == 2
        assert response.total_features == 10
        assert response.features_by_type[FeatureType.INTEGER] == 5
        assert response.features_by_status[FeatureStatus.ACTIVE] == 8


class TestFeatureValidationResult:
    """Tests for FeatureValidationResult model."""

    def test_valid_result(self):
        """Test creating a valid validation result."""
        result = FeatureValidationResult(
            feature_name="user_age",
            is_valid=True,
            errors=[],
            warnings=["Value is at minimum threshold"],
        )

        assert result.feature_name == "user_age"
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 1

    def test_invalid_result(self):
        """Test creating an invalid validation result."""
        result = FeatureValidationResult(
            feature_name="user_age",
            is_valid=False,
            errors=["Value must be positive", "Value exceeds maximum"],
            warnings=[],
        )

        assert result.feature_name == "user_age"
        assert result.is_valid is False
        assert len(result.errors) == 2
        assert len(result.warnings) == 0
