"""Tests for feature store core service."""

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from ..core import FeatureStoreService
from ..models import (
    Feature,
    FeatureDefinitionCreate,
    FeatureDefinitionUpdate,
    FeatureStatus,
    FeatureType,
    FeatureValueWrite,
)


@pytest.fixture
def mock_db_manager():
    """Mock database manager."""
    db_manager = Mock()
    db_manager.get_session = AsyncMock()
    return db_manager


@pytest.fixture
def feature_store_service(mock_db_manager):
    """Feature store service with mocked database."""
    return FeatureStoreService(mock_db_manager)


@pytest.fixture
def sample_feature_data():
    """Sample feature definition data."""
    return FeatureDefinitionCreate(
        name="user_age",
        feature_group="user_demographics",
        description="User age in years",
        feature_type=FeatureType.INTEGER,
        default_value=25,
        validation_rules={"min_value": 0, "max_value": 120},
        tags=["demographic", "user"],
        owner="data_team",
    )


@pytest.fixture
def sample_feature_model():
    """Sample feature database model."""
    return Feature(
        id=1,
        name="user_age",
        feature_group="user_demographics",
        description="User age in years",
        feature_type=FeatureType.INTEGER,
        status=FeatureStatus.ACTIVE,
        default_value="25",
        validation_rules='{"min_value": 0, "max_value": 120}',
        tags='["demographic", "user"]',
        owner="data_team",
        version=1,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


class TestFeatureStoreService:
    """Tests for FeatureStoreService."""

    @pytest.mark.asyncio
    async def test_create_feature(
        self, feature_store_service, mock_db_manager, sample_feature_data
    ):
        """Test creating a feature."""
        # Mock session and database operations
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock the created feature - not used directly in test

        mock_session.refresh = AsyncMock()
        mock_session.refresh.return_value = None

        # Execute
        with patch.object(
            feature_store_service, "_feature_to_response"
        ) as mock_to_response:
            mock_to_response.return_value = Mock()  # Mock response
            await feature_store_service.create_feature(sample_feature_data)

            # Verify database operations
            mock_session.add.assert_called_once()
            mock_session.commit.assert_called_once()
            mock_session.refresh.assert_called_once()
            mock_to_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_feature_existing(
        self, feature_store_service, mock_db_manager, sample_feature_model
    ):
        """Test getting an existing feature."""
        # Mock session and database operations
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock query result
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = sample_feature_model
        mock_session.execute.return_value = mock_result

        # Execute
        with patch.object(
            feature_store_service, "_feature_to_response"
        ) as mock_to_response:
            mock_to_response.return_value = Mock()
            result = await feature_store_service.get_feature("user_age")

            # Verify
            assert result is not None
            mock_session.execute.assert_called_once()
            mock_to_response.assert_called_once_with(sample_feature_model)

    @pytest.mark.asyncio
    async def test_get_feature_not_found(self, feature_store_service, mock_db_manager):
        """Test getting a non-existent feature."""
        # Mock session and database operations
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock query result - no feature found
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        # Execute
        result = await feature_store_service.get_feature("non_existent")

        # Verify
        assert result is None
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_feature(
        self, feature_store_service, mock_db_manager, sample_feature_model
    ):
        """Test updating a feature."""
        # Mock session and database operations
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock query result
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = sample_feature_model
        mock_session.execute.return_value = mock_result

        # Update data
        update_data = FeatureDefinitionUpdate(
            description="Updated description",
            status=FeatureStatus.DEPRECATED,
        )

        # Execute
        with patch.object(
            feature_store_service, "_feature_to_response"
        ) as mock_to_response:
            mock_to_response.return_value = Mock()
            result = await feature_store_service.update_feature("user_age", update_data)

            # Verify
            assert result is not None
            mock_session.commit.assert_called_once()
            mock_session.refresh.assert_called_once()
            mock_to_response.assert_called_once()

            # Verify feature was updated
            assert sample_feature_model.description == "Updated description"
            assert sample_feature_model.status == FeatureStatus.DEPRECATED

    @pytest.mark.asyncio
    async def test_delete_feature(
        self, feature_store_service, mock_db_manager, sample_feature_model
    ):
        """Test deleting a feature."""
        # Mock session and database operations
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock query result
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = sample_feature_model
        mock_session.execute.return_value = mock_result

        # Execute
        result = await feature_store_service.delete_feature("user_age")

        # Verify
        assert result is True
        mock_session.delete.assert_called_once_with(sample_feature_model)
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_feature_value(
        self, feature_store_service, mock_db_manager, sample_feature_model
    ):
        """Test writing a feature value."""
        # Mock session and database operations
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock feature lookup
        with patch.object(
            feature_store_service, "_get_feature_by_name"
        ) as mock_get_feature:
            mock_get_feature.return_value = sample_feature_model

            # Mock validation
            with patch.object(
                feature_store_service, "_validate_feature_value"
            ) as mock_validate:
                mock_validation_result = Mock()
                mock_validation_result.is_valid = True
                mock_validate.return_value = mock_validation_result

                # Mock response conversion
                with patch.object(
                    feature_store_service, "_feature_value_to_response"
                ) as mock_to_response:
                    mock_to_response.return_value = Mock()

                    # Test data
                    value_data = FeatureValueWrite(
                        feature_name="user_age",
                        entity_id="user_123",
                        value=30,
                    )

                    # Execute
                    result = await feature_store_service.write_feature_value(value_data)

                    # Verify
                    assert result is not None
                    mock_session.add.assert_called_once()
                    mock_session.commit.assert_called_once()
                    mock_session.refresh.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_feature_value_invalid_feature(
        self, feature_store_service, mock_db_manager
    ):
        """Test writing a value for non-existent feature."""
        # Mock session
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock feature lookup - no feature found
        with patch.object(
            feature_store_service, "_get_feature_by_name"
        ) as mock_get_feature:
            mock_get_feature.return_value = None

            # Test data
            value_data = FeatureValueWrite(
                feature_name="non_existent",
                entity_id="user_123",
                value=30,
            )

            # Execute and verify exception
            with pytest.raises(
                ValueError, match="Feature 'non_existent' does not exist"
            ):
                await feature_store_service.write_feature_value(value_data)

    @pytest.mark.asyncio
    async def test_write_feature_value_validation_error(
        self, feature_store_service, mock_db_manager, sample_feature_model
    ):
        """Test writing an invalid feature value."""
        # Mock session
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock feature lookup
        with patch.object(
            feature_store_service, "_get_feature_by_name"
        ) as mock_get_feature:
            mock_get_feature.return_value = sample_feature_model

            # Mock validation - invalid
            with patch.object(
                feature_store_service, "_validate_feature_value"
            ) as mock_validate:
                mock_validation_result = Mock()
                mock_validation_result.is_valid = False
                mock_validation_result.errors = ["Value is too large"]
                mock_validate.return_value = mock_validation_result

                # Test data
                value_data = FeatureValueWrite(
                    feature_name="user_age",
                    entity_id="user_123",
                    value=150,  # Invalid age
                )

                # Execute and verify exception
                with pytest.raises(
                    ValueError, match="Invalid value: Value is too large"
                ):
                    await feature_store_service.write_feature_value(value_data)

    @pytest.mark.asyncio
    async def test_serve_features(
        self, feature_store_service, mock_db_manager, sample_feature_model
    ):
        """Test serving features."""
        # Mock session
        mock_session = AsyncMock()
        mock_db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock read_feature_values
        mock_feature_values = [
            Mock(entity_id="user_123", feature_name="user_age", value=30)
        ]
        with patch.object(feature_store_service, "read_feature_values") as mock_read:
            mock_read.return_value = mock_feature_values

            # Mock feature lookup for defaults
            with patch.object(
                feature_store_service, "_get_feature_by_name"
            ) as mock_get_feature:
                mock_get_feature.return_value = sample_feature_model

                # Execute
                result = await feature_store_service.serve_features(
                    feature_names=["user_age", "user_name"],
                    entity_id="user_123",
                )

                # Verify
                assert result.entity_id == "user_123"
                assert "user_age" in result.features
                assert result.features["user_age"] == 30

    @pytest.mark.asyncio
    async def test_validate_feature_value_type_validation(self, feature_store_service):
        """Test feature value type validation."""
        # Create feature with integer type
        feature = Feature(
            name="user_age",
            feature_type=FeatureType.INTEGER,
            validation_rules=None,
        )

        # Test valid integer
        result = await feature_store_service._validate_feature_value(feature, 30)
        assert result.is_valid is True
        assert len(result.errors) == 0

        # Test invalid type (string instead of integer)
        result = await feature_store_service._validate_feature_value(feature, "thirty")
        assert result.is_valid is False
        assert any("Expected integer" in error for error in result.errors)

    @pytest.mark.asyncio
    async def test_validate_feature_value_custom_rules(self, feature_store_service):
        """Test feature value custom validation rules."""
        # Create feature with validation rules
        feature = Feature(
            name="user_age",
            feature_type=FeatureType.INTEGER,
            validation_rules='{"min_value": 0, "max_value": 120}',
        )

        # Test valid value
        result = await feature_store_service._validate_feature_value(feature, 30)
        assert result.is_valid is True

        # Test value below minimum
        result = await feature_store_service._validate_feature_value(feature, -5)
        assert result.is_valid is False
        assert any("below minimum" in error for error in result.errors)

        # Test value above maximum
        result = await feature_store_service._validate_feature_value(feature, 150)
        assert result.is_valid is False
        assert any("above maximum" in error for error in result.errors)

    def test_feature_to_response_conversion(
        self, feature_store_service, sample_feature_model
    ):
        """Test converting Feature model to response."""
        # Execute
        response = feature_store_service._feature_to_response(sample_feature_model)

        # Verify basic fields
        assert response.name == sample_feature_model.name
        assert response.feature_group == sample_feature_model.feature_group
        assert response.feature_type == sample_feature_model.feature_type

        # Verify JSON field parsing
        assert response.default_value == 25  # Parsed from JSON string
        assert response.validation_rules == {"min_value": 0, "max_value": 120}
        assert response.tags == ["demographic", "user"]
