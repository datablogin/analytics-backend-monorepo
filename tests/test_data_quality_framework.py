"""Tests for the data quality framework."""

import tempfile
from unittest.mock import Mock

import pandas as pd
import pytest

from libs.data_processing import (
    AlertChannel,
    AlertingConfig,
    AlertRule,
    AlertSeverity,
    DataLineageTracker,
    DataProfiler,
    DataQualityAlerting,
    DataQualityConfig,
    DataQualityFramework,
    create_common_expectations,
    create_default_alert_rules,
)


class TestDataQualityFramework:
    """Test data quality validation framework."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", None, "Eve"],
                "age": [25, 30, 35, 40, 28],
                "email": [
                    "alice@test.com",
                    "bob@test.com",
                    "charlie@test.com",
                    "dave@test.com",
                    "eve@test.com",
                ],
                "created_at": pd.to_datetime(
                    [
                        "2023-01-01",
                        "2023-01-02",
                        "2023-01-03",
                        "2023-01-04",
                        "2023-01-05",
                    ]
                ),
            }
        )

    @pytest.fixture
    def temp_ge_context(self):
        """Create temporary Great Expectations context."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = DataQualityConfig(context_root_dir=temp_dir)
            yield config

    def test_data_quality_framework_initialization(self, temp_ge_context):
        """Test framework initialization."""
        framework = DataQualityFramework(temp_ge_context)
        assert framework.config == temp_ge_context
        assert hasattr(framework, "expectation_suites")

    def test_create_expectation_suite(self, temp_ge_context, sample_dataframe):
        """Test expectation suite creation."""
        framework = DataQualityFramework(temp_ge_context)

        expectations = create_common_expectations(
            table_name="test_table",
            columns=sample_dataframe.columns.tolist(),
            primary_key_columns=["id"],
            not_null_columns=["id", "email"],
            unique_columns=["id", "email"],
        )

        suite_name = framework.create_expectation_suite("test_suite", expectations)
        assert suite_name == "test_suite"
        assert "test_suite" in framework.expectation_suites
        assert len(framework.expectation_suites["test_suite"]) > 0

    def test_validate_dataframe(self, temp_ge_context, sample_dataframe):
        """Test DataFrame validation."""
        framework = DataQualityFramework(temp_ge_context)

        # Create simple expectations
        expectations = [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1, "max_value": 10},
            },
            {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}},
        ]

        framework.create_expectation_suite("test_validation_suite", expectations)
        result = framework.validate_dataframe(
            sample_dataframe, "test_validation_suite", "test_dataset"
        )

        assert result.dataset_name == "test_dataset"
        assert result.expectations_count == len(expectations)
        assert isinstance(result.success, bool)
        assert isinstance(result.success_rate, float)

    def test_common_expectations_creation(self):
        """Test common expectations helper function."""
        columns = ["id", "name", "email"]
        expectations = create_common_expectations(
            table_name="users",
            columns=columns,
            primary_key_columns=["id"],
            not_null_columns=["id", "email"],
            unique_columns=["id", "email"],
        )

        assert len(expectations) > 0

        # Check for required expectations
        expectation_types = {exp["expectation_type"] for exp in expectations}
        assert "expect_table_columns_to_match_ordered_list" in expectation_types
        assert "expect_column_values_to_not_be_null" in expectation_types
        assert "expect_column_values_to_be_unique" in expectation_types


class TestDataProfiler:
    """Test data profiling functionality."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for profiling."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 1],  # Duplicate row (row 0 and 5 identical)
                "name": [
                    "Alice",
                    "Bob",
                    None,
                    "Dave",
                    "Eve",
                    "Alice",
                ],  # Missing value + duplicate
                "age": [25, 30, 35, 40, 28, 25],  # Duplicate row
                "score": [95.5, 87.2, 92.1, 88.9, 94.3, 95.5],  # Duplicate row
                "active": [True, False, True, True, False, True],  # Duplicate row
                "created_at": pd.to_datetime(
                    [
                        "2023-01-01",
                        "2023-01-02",
                        "2023-01-03",
                        "2023-01-04",
                        "2023-01-05",
                        "2023-01-01",  # Duplicate row
                    ]
                ),
            }
        )

    def test_profile_dataframe(self, sample_dataframe):
        """Test basic DataFrame profiling."""
        profiler = DataProfiler()
        profile = profiler.profile_dataframe(sample_dataframe, "test_dataset")

        assert profile.dataset_name == "test_dataset"
        assert profile.row_count == len(sample_dataframe)
        assert profile.column_count == len(sample_dataframe.columns)
        assert profile.missing_cells > 0  # We have missing values
        assert profile.duplicate_rows > 0  # We have duplicates
        assert len(profile.columns) == len(sample_dataframe.columns)

    def test_numeric_column_profiling(self, sample_dataframe):
        """Test numeric column profiling."""
        profiler = DataProfiler()
        profile = profiler.profile_dataframe(sample_dataframe, "test_dataset")

        age_profile = profile.columns["age"]
        assert age_profile["dtype"] == "int64"
        assert "min" in age_profile["statistics"]
        assert "max" in age_profile["statistics"]
        assert "mean" in age_profile["statistics"]
        assert "std" in age_profile["statistics"]

    def test_categorical_column_profiling(self, sample_dataframe):
        """Test categorical column profiling."""
        profiler = DataProfiler()
        profile = profiler.profile_dataframe(sample_dataframe, "test_dataset")

        name_profile = profile.columns["name"]
        assert "max_length" in name_profile["statistics"]
        assert "top_categories" in name_profile["statistics"]
        assert name_profile["missing_count"] == 1

    def test_datetime_column_profiling(self, sample_dataframe):
        """Test datetime column profiling."""
        profiler = DataProfiler()
        profile = profiler.profile_dataframe(sample_dataframe, "test_dataset")

        date_profile = profile.columns["created_at"]
        assert "min" in date_profile["statistics"]
        assert "max" in date_profile["statistics"]
        assert "range_days" in date_profile["statistics"]

    def test_warning_generation(self, sample_dataframe):
        """Test data quality warning generation."""
        profiler = DataProfiler()
        profile = profiler.profile_dataframe(sample_dataframe, "test_dataset")

        # Should generate warnings for duplicates and missing data
        assert len(profile.warnings) > 0

    def test_profile_comparison(self, sample_dataframe):
        """Test profile comparison for drift detection."""
        profiler = DataProfiler()

        # Create two similar profiles
        profile1 = profiler.profile_dataframe(sample_dataframe, "dataset_v1")

        # Modify DataFrame slightly
        modified_df = sample_dataframe.copy()
        modified_df.loc[0, "name"] = None  # Add more missing data
        profile2 = profiler.profile_dataframe(modified_df, "dataset_v2")

        comparison = profiler.compare_profiles(profile1, profile2)

        assert comparison["dataset1"] == "dataset_v1"
        assert comparison["dataset2"] == "dataset_v2"
        assert "column_changes" in comparison
        assert "warnings" in comparison


class TestDataLineageTracker:
    """Test data lineage tracking."""

    @pytest.fixture
    def lineage_tracker(self):
        """Create lineage tracker instance."""
        return DataLineageTracker()

    def test_register_asset(self, lineage_tracker):
        """Test asset registration."""
        asset_id = lineage_tracker.register_asset(
            name="test_table", asset_type="table", database="test_db", schema="public"
        )

        assert asset_id in lineage_tracker.graph.assets
        asset = lineage_tracker.graph.assets[asset_id]
        assert asset.name == "test_table"
        assert asset.type == "table"

    def test_register_transformation(self, lineage_tracker):
        """Test transformation registration."""
        trans_id = lineage_tracker.register_transformation(
            name="etl_process",
            transformation_type="etl",
            description="Test ETL process",
        )

        assert trans_id in lineage_tracker.graph.transformations
        transformation = lineage_tracker.graph.transformations[trans_id]
        assert transformation.name == "etl_process"
        assert transformation.type == "etl"

    def test_add_lineage_relationship(self, lineage_tracker):
        """Test lineage relationship creation."""
        source_id = lineage_tracker.register_asset("source_table", "table")
        target_id = lineage_tracker.register_asset("target_table", "table")
        trans_id = lineage_tracker.register_transformation("transform", "etl")

        lineage_tracker.add_lineage(source_id, target_id, trans_id)

        assert len(lineage_tracker.graph.edges) == 1
        edge = lineage_tracker.graph.edges[0]
        assert edge.source_asset_id == source_id
        assert edge.target_asset_id == target_id
        assert edge.transformation_id == trans_id

    def test_upstream_downstream_tracking(self, lineage_tracker):
        """Test upstream and downstream asset tracking."""
        # Create lineage: A -> B -> C
        asset_a = lineage_tracker.register_asset("A", "table")
        asset_b = lineage_tracker.register_asset("B", "table")
        asset_c = lineage_tracker.register_asset("C", "table")

        lineage_tracker.add_lineage(asset_a, asset_b)
        lineage_tracker.add_lineage(asset_b, asset_c)

        # Test upstream
        upstream_c = lineage_tracker.get_upstream_assets(asset_c)
        assert asset_b in upstream_c
        assert asset_a in upstream_c

        # Test downstream
        downstream_a = lineage_tracker.get_downstream_assets(asset_a)
        assert asset_b in downstream_a
        assert asset_c in downstream_a

    def test_impact_analysis(self, lineage_tracker):
        """Test impact analysis functionality."""
        source_id = lineage_tracker.register_asset("critical_table", "table")
        target1_id = lineage_tracker.register_asset("report1", "report")
        target2_id = lineage_tracker.register_asset("dashboard", "dashboard")

        lineage_tracker.add_lineage(source_id, target1_id)
        lineage_tracker.add_lineage(source_id, target2_id)

        impact = lineage_tracker.analyze_impact(source_id)

        assert impact["total_downstream_assets"] == 2
        assert "impact_by_type" in impact
        assert "risk_level" in impact

    def test_lineage_validation(self, lineage_tracker):
        """Test lineage integrity validation."""
        # Create valid lineage
        asset_id = lineage_tracker.register_asset("test_asset", "table")

        # Should have no issues
        issues = lineage_tracker.validate_lineage_integrity()
        assert len(issues) == 0

        # Add invalid edge manually
        from libs.data_processing.lineage import LineageEdge

        invalid_edge = LineageEdge(
            source_asset_id="nonexistent", target_asset_id=asset_id
        )
        lineage_tracker.graph.edges.append(invalid_edge)

        # Should detect the issue
        issues = lineage_tracker.validate_lineage_integrity()
        assert len(issues) > 0


class TestDataQualityAlerting:
    """Test data quality alerting system."""

    @pytest.fixture
    def alerting_config(self):
        """Create alerting configuration."""
        return AlertingConfig(enabled=True, default_channels=[AlertChannel.LOG])

    @pytest.fixture
    def alerting_system(self, alerting_config):
        """Create alerting system instance."""
        return DataQualityAlerting(alerting_config)

    def test_add_alert_rule(self, alerting_system):
        """Test adding alert rules."""
        rule = AlertRule(
            id="test_rule",
            name="Test Rule",
            description="Test alert rule",
            severity=AlertSeverity.MEDIUM,
            channels=[AlertChannel.LOG],
            conditions={"min_success_rate": 90.0},
        )

        alerting_system.add_rule(rule)
        assert "test_rule" in alerting_system.rules
        assert alerting_system.rules["test_rule"] == rule

    def test_validation_result_evaluation(self, alerting_system):
        """Test evaluation of validation results."""
        # Add rule that should trigger
        rule = AlertRule(
            id="low_success_rule",
            name="Low Success Rate",
            description="Alert on low success rate",
            severity=AlertSeverity.HIGH,
            channels=[AlertChannel.LOG],
            conditions={"min_success_rate": 95.0},
        )
        alerting_system.add_rule(rule)

        # Create validation result that should trigger alert
        validation_result = {
            "dataset_name": "test_dataset",
            "success_rate": 80.0,
            "failure_count": 5,
            "success": False,
        }

        alerts = alerting_system.evaluate_validation_result(validation_result)

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.rule_id == "low_success_rule"
        assert alert.severity == AlertSeverity.HIGH
        assert alert.dataset_name == "test_dataset"

    def test_cooldown_mechanism(self, alerting_system):
        """Test alert cooldown mechanism."""
        rule = AlertRule(
            id="cooldown_rule",
            name="Cooldown Test",
            description="Test cooldown",
            severity=AlertSeverity.MEDIUM,
            channels=[AlertChannel.LOG],
            conditions={"min_success_rate": 95.0},
            cooldown_minutes=60,
        )
        alerting_system.add_rule(rule)

        validation_result = {
            "dataset_name": "test_dataset",
            "success_rate": 80.0,
            "failure_count": 5,
            "success": False,
        }

        # First alert should be sent
        alerts1 = alerting_system.evaluate_validation_result(validation_result)
        assert len(alerts1) == 1

        # Second alert should be suppressed due to cooldown
        alerts2 = alerting_system.evaluate_validation_result(validation_result)
        assert len(alerts2) == 0

    def test_log_alert_sending(self, alerting_system):
        """Test sending alerts via logging."""
        mock_logger = Mock()
        # Patch the logger instance directly on the alerting system
        original_logger = alerting_system.logger
        alerting_system.logger = mock_logger

        rule = AlertRule(
            id="log_rule",
            name="Log Alert",
            description="Test log alert",
            severity=AlertSeverity.LOW,
            channels=[AlertChannel.LOG],
            conditions={"min_success_rate": 95.0},
        )
        alerting_system.add_rule(rule)

        validation_result = {
            "dataset_name": "test_dataset",
            "success_rate": 80.0,
            "failure_count": 5,
            "success": False,
        }

        alerts = alerting_system.evaluate_validation_result(validation_result)
        alerting_system.send_alerts(alerts)

        # Restore original logger
        alerting_system.logger = original_logger

        # Verify logger was called
        assert mock_logger.warning.called

    def test_default_alert_rules_creation(self):
        """Test creation of default alert rules."""
        rules = create_default_alert_rules()

        assert len(rules) > 0
        assert all(isinstance(rule, AlertRule) for rule in rules)

        # Check for expected rule types
        rule_names = {rule.name for rule in rules}
        assert "Low Success Rate" in rule_names
        assert "Multiple Validation Failures" in rule_names

    def test_alert_acknowledgment_and_resolution(self, alerting_system):
        """Test alert acknowledgment and resolution."""
        rule = AlertRule(
            id="ack_rule",
            name="Acknowledgment Test",
            description="Test acknowledgment",
            severity=AlertSeverity.MEDIUM,
            channels=[AlertChannel.LOG],
            conditions={"min_success_rate": 95.0},
        )
        alerting_system.add_rule(rule)

        validation_result = {
            "dataset_name": "test_dataset",
            "success_rate": 80.0,
            "failure_count": 5,
            "success": False,
        }

        alerts = alerting_system.evaluate_validation_result(validation_result)
        alert_id = alerts[0].id

        # Test acknowledgment
        assert alerting_system.acknowledge_alert(alert_id)
        assert alerts[0].status == "acknowledged"

        # Test resolution
        assert alerting_system.resolve_alert(alert_id)
        assert alerts[0].status == "resolved"

        # Test invalid alert ID
        assert not alerting_system.acknowledge_alert("invalid_id")


@pytest.mark.integration
class TestDataQualityIntegration:
    """Integration tests for the complete data quality framework."""

    @pytest.fixture
    def sample_dataset(self):
        """Create sample dataset for integration testing."""
        return pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "username": ["alice", "bob", "charlie", "dave", "eve"],
                "email": [
                    "alice@test.com",
                    "bob@test.com",
                    None,
                    "dave@test.com",
                    "eve@test.com",
                ],
                "age": [25, 30, 35, 40, 28],
                "registration_date": pd.to_datetime(
                    [
                        "2023-01-01",
                        "2023-01-02",
                        "2023-01-03",
                        "2023-01-04",
                        "2023-01-05",
                    ]
                ),
                "is_active": [True, True, False, True, True],
            }
        )

    def test_end_to_end_data_quality_pipeline(self, sample_dataset):
        """Test complete data quality pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Initialize components
            dq_config = DataQualityConfig(context_root_dir=temp_dir)
            dq_framework = DataQualityFramework(dq_config)
            profiler = DataProfiler()
            lineage_tracker = DataLineageTracker()
            alerting_config = AlertingConfig(enabled=True)
            alerting_system = DataQualityAlerting(alerting_config)

            # Add default alert rules
            for rule in create_default_alert_rules():
                alerting_system.add_rule(rule)

            # Step 1: Profile the data
            profile = profiler.profile_dataframe(sample_dataset, "users_table")
            assert profile.dataset_name == "users_table"
            assert profile.row_count == 5

            # Step 2: Create and run validation
            expectations = create_common_expectations(
                table_name="users",
                columns=sample_dataset.columns.tolist(),
                primary_key_columns=["user_id"],
                not_null_columns=["user_id", "username"],
                unique_columns=["user_id", "username"],
            )

            dq_framework.create_expectation_suite("users_suite", expectations)
            validation_result = dq_framework.validate_dataframe(
                sample_dataset, "users_suite", "users_table"
            )

            # Step 3: Check for alerts
            alerting_system.evaluate_validation_result(validation_result.model_dump())

            # Step 4: Register lineage
            source_id = lineage_tracker.register_asset(
                "raw_users", "file", location="/data/raw/users.csv"
            )
            target_id = lineage_tracker.register_asset(
                "users_table", "table", database="analytics"
            )
            trans_id = lineage_tracker.register_transformation("user_ingestion", "etl")
            lineage_tracker.add_lineage(source_id, target_id, trans_id)

            # Verify integration
            assert validation_result.dataset_name == "users_table"
            assert len(lineage_tracker.graph.assets) == 2
            assert len(lineage_tracker.graph.transformations) == 1
            assert len(lineage_tracker.graph.edges) == 1

            # Test impact analysis
            impact = lineage_tracker.analyze_impact(source_id)
            assert impact["total_downstream_assets"] == 1
