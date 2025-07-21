"""Data quality validation framework using Great Expectations."""

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog
from pydantic import BaseModel, Field

# For now, create a simplified implementation that doesn't require Great Expectations
# This allows the framework to work without the complex GE setup
logger = structlog.get_logger(__name__)


class DataQualityResult(BaseModel):
    """Result of data quality validation."""

    success: bool
    validation_time: datetime
    dataset_name: str
    expectations_count: int
    success_count: int
    failure_count: int
    success_rate: float
    failures: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DataQualityConfig(BaseModel):
    """Configuration for data quality validation."""

    context_root_dir: str = "great_expectations"
    datasource_name: str = "default_datasource"
    data_connector_name: str = "default_runtime_data_connector"
    expectation_suite_name: str = "default_suite"
    checkpoint_name: str = "default_checkpoint"
    store_backend: str = "filesystem"


class DataQualityFramework:
    """Great Expectations-based data quality validation framework."""

    def __init__(self, config: DataQualityConfig):
        self.config = config
        self.context = self._initialize_context()
        self.logger = structlog.get_logger(__name__)

    def _initialize_context(self):
        """Initialize Great Expectations data context."""
        try:
            context_dir = Path(self.config.context_root_dir)
            if context_dir.exists():
                context = get_context(context_root_dir=str(context_dir))
            else:
                context = get_context(context_root_dir=str(context_dir))
                self._setup_default_datasource(context)
            return context
        except DataContextError as e:
            self.logger.error("Failed to initialize data context", error=str(e))
            raise

    def _setup_default_datasource(self, context) -> None:
        """Set up default runtime datasource."""
        datasource_config = {
            "name": self.config.datasource_name,
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "data_connectors": {
                self.config.data_connector_name: {
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                    "batch_identifiers": ["default_identifier_name"],
                }
            },
        }
        context.add_datasource(**datasource_config)

    def create_expectation_suite(
        self, suite_name: str, expectations: list[dict[str, Any]]
    ) -> ExpectationSuite:
        """Create or update an expectation suite."""
        try:
            suite = self.context.create_expectation_suite(
                expectation_suite_name=suite_name, overwrite_existing=True
            )

            for exp_config in expectations:
                expectation = ExpectationConfiguration(**exp_config)
                suite.add_expectation(expectation)

            self.context.save_expectation_suite(suite)
            self.logger.info(
                "Created expectation suite",
                suite_name=suite_name,
                expectations_count=len(expectations),
            )
            return suite
        except Exception as e:
            self.logger.error(
                "Failed to create expectation suite",
                suite_name=suite_name,
                error=str(e),
            )
            raise

    def validate_dataframe(
        self, df: pd.DataFrame, suite_name: str, dataset_name: str = "default_dataset"
    ) -> DataQualityResult:
        """Validate a pandas DataFrame against an expectation suite."""
        try:
            validation_time = datetime.utcnow()

            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name=self.config.datasource_name,
                data_connector_name=self.config.data_connector_name,
                data_asset_name=dataset_name,
                runtime_parameters={"batch_data": df},
                batch_identifiers={"default_identifier_name": dataset_name},
            )

            # Run validation
            validator = self.context.get_validator(
                batch_request=batch_request, expectation_suite_name=suite_name
            )

            validation_result = validator.validate()

            # Process results
            success = validation_result.success
            results = validation_result.results

            success_count = sum(1 for r in results if r.success)
            failure_count = len(results) - success_count
            success_rate = success_count / len(results) if results else 0.0

            failures = [
                {
                    "expectation_type": r.expectation_config.expectation_type,
                    "kwargs": r.expectation_config.kwargs,
                    "result": r.result,
                }
                for r in results
                if not r.success
            ]

            result = DataQualityResult(
                success=success,
                validation_time=validation_time,
                dataset_name=dataset_name,
                expectations_count=len(results),
                success_count=success_count,
                failure_count=failure_count,
                success_rate=success_rate,
                failures=failures,
                metadata={
                    "suite_name": suite_name,
                    "row_count": len(df),
                    "column_count": len(df.columns),
                },
            )

            self.logger.info(
                "Data validation completed",
                dataset_name=dataset_name,
                suite_name=suite_name,
                success=success,
                success_rate=success_rate,
            )

            return result

        except Exception as e:
            self.logger.error(
                "Data validation failed",
                dataset_name=dataset_name,
                suite_name=suite_name,
                error=str(e),
            )
            raise

    def create_data_quality_checkpoint(
        self,
        checkpoint_name: str,
        suite_name: str,
        validation_operators: list[str] | None = None,
    ) -> None:
        """Create a checkpoint for automated validation."""
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "template_name": None,
            "module_name": "great_expectations.checkpoint",
            "class_name": "SimpleCheckpoint",
            "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
            "expectation_suite_name": suite_name,
            "batch_request": {},
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
            "evaluation_parameters": {},
            "runtime_configuration": {},
            "validations": [],
        }

        if validation_operators:
            checkpoint_config["action_list"].extend(
                [
                    {"name": op, "action": {"class_name": f"{op}Action"}}
                    for op in validation_operators
                ]
            )

        self.context.add_checkpoint(**checkpoint_config)
        self.logger.info("Created checkpoint", checkpoint_name=checkpoint_name)

    def get_validation_history(
        self, suite_name: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Get validation history for a suite."""
        try:
            store = self.context.stores["validations_store"]
            keys = store.list_keys()

            # Filter by suite name and sort by timestamp
            suite_keys = [
                key
                for key in keys
                if key.expectation_suite_identifier.expectation_suite_name == suite_name
            ]
            suite_keys.sort(key=lambda k: k.run_id.run_time, reverse=True)

            history = []
            for key in suite_keys[:limit]:
                validation_result = store.get(key)
                history.append(
                    {
                        "run_id": key.run_id.run_name,
                        "run_time": key.run_id.run_time,
                        "success": validation_result.success,
                        "statistics": validation_result.statistics,
                    }
                )

            return history
        except Exception as e:
            self.logger.error(
                "Failed to get validation history", suite_name=suite_name, error=str(e)
            )
            return []


def create_common_expectations(
    table_name: str,
    columns: list[str],
    primary_key_columns: list[str] | None = None,
    not_null_columns: list[str] | None = None,
    unique_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Create common data quality expectations for a table."""
    expectations = []

    # Table-level expectations
    expectations.append(
        {
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {"column_list": columns},
        }
    )

    expectations.append(
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1},
        }
    )

    # Column-level expectations
    for column in columns:
        expectations.append(
            {"expectation_type": "expect_column_to_exist", "kwargs": {"column": column}}
        )

    # Primary key constraints
    if primary_key_columns:
        for pk_col in primary_key_columns:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": pk_col},
                }
            )
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": pk_col},
                }
            )

    # Not null constraints
    if not_null_columns:
        for col in not_null_columns:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col},
                }
            )

    # Unique constraints
    if unique_columns:
        for col in unique_columns:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": col},
                }
            )

    return expectations
