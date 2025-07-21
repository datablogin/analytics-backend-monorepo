"""Simplified data quality validation framework."""

from datetime import datetime
from typing import Any

import pandas as pd
import structlog
from pydantic import BaseModel, Field

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

    context_root_dir: str = "data_quality"
    fail_on_validation_error: bool = False


class ExpectationRule(BaseModel):
    """Simple expectation rule."""

    expectation_type: str
    column: str | None = None
    kwargs: dict[str, Any] = Field(default_factory=dict)


class SimpleDataQualityFramework:
    """Simplified data quality validation framework."""

    def __init__(self, config: DataQualityConfig):
        self.config = config
        self.logger = structlog.get_logger(__name__)
        self.expectation_suites: dict[str, list[ExpectationRule]] = {}

    def create_expectation_suite(
        self, suite_name: str, expectations: list[dict[str, Any]]
    ) -> str:
        """Create an expectation suite."""
        rules = []
        for exp in expectations:
            rule = ExpectationRule(
                expectation_type=exp["expectation_type"],
                column=exp.get("kwargs", {}).get("column"),
                kwargs=exp.get("kwargs", {}),
            )
            rules.append(rule)

        self.expectation_suites[suite_name] = rules
        self.logger.info(
            "Created expectation suite",
            suite_name=suite_name,
            expectations_count=len(rules),
        )
        return suite_name

    def validate_dataframe(
        self, df: pd.DataFrame, suite_name: str, dataset_name: str = "default_dataset"
    ) -> DataQualityResult:
        """Validate a pandas DataFrame against an expectation suite."""
        try:
            validation_time = datetime.utcnow()

            if suite_name not in self.expectation_suites:
                raise ValueError(f"Expectation suite '{suite_name}' not found")

            expectations = self.expectation_suites[suite_name]
            results = []
            failures = []

            for expectation in expectations:
                try:
                    result = self._validate_expectation(df, expectation)
                    results.append(result)
                    if not result["success"]:
                        failures.append(
                            {
                                "expectation_type": expectation.expectation_type,
                                "kwargs": expectation.kwargs,
                                "result": result,
                            }
                        )
                except Exception as e:
                    results.append({"success": False, "error": str(e)})
                    failures.append(
                        {
                            "expectation_type": expectation.expectation_type,
                            "kwargs": expectation.kwargs,
                            "result": {"error": str(e)},
                        }
                    )

            success_count = sum(1 for r in results if r.get("success", False))
            failure_count = len(results) - success_count
            success_rate = success_count / len(results) if results else 0.0
            overall_success = failure_count == 0

            result = DataQualityResult(
                success=overall_success,
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
                success=overall_success,
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

    def _validate_expectation(
        self, df: pd.DataFrame, expectation: ExpectationRule
    ) -> dict[str, Any]:
        """Validate a single expectation."""
        exp_type = expectation.expectation_type
        kwargs = expectation.kwargs

        if exp_type == "expect_table_row_count_to_be_between":
            min_val = kwargs.get("min_value", 0)
            max_val = kwargs.get("max_value", float("inf"))
            row_count = len(df)
            success = min_val <= row_count <= max_val
            return {
                "success": success,
                "observed_value": row_count,
                "expected_range": [min_val, max_val],
            }

        elif exp_type == "expect_column_to_exist":
            column = kwargs.get("column")
            success = column in df.columns
            return {
                "success": success,
                "observed_value": column in df.columns,
                "column": column,
            }

        elif exp_type == "expect_column_values_to_not_be_null":
            column = kwargs.get("column")
            if column not in df.columns:
                return {"success": False, "error": f"Column {column} not found"}
            null_count = df[column].isnull().sum()
            success = null_count == 0
            return {
                "success": success,
                "observed_value": {
                    "null_count": int(null_count),
                    "null_percent": float(null_count / len(df) * 100),
                },
                "column": column,
            }

        elif exp_type == "expect_column_values_to_be_unique":
            column = kwargs.get("column")
            if column not in df.columns:
                return {"success": False, "error": f"Column {column} not found"}
            duplicate_count = df[column].duplicated().sum()
            success = duplicate_count == 0
            return {
                "success": success,
                "observed_value": {"duplicate_count": int(duplicate_count)},
                "column": column,
            }

        elif exp_type == "expect_table_columns_to_match_ordered_list":
            expected_columns = kwargs.get("column_list", [])
            actual_columns = df.columns.tolist()
            success = expected_columns == actual_columns
            return {
                "success": success,
                "observed_value": actual_columns,
                "expected_value": expected_columns,
            }

        else:
            return {
                "success": False,
                "error": f"Unsupported expectation type: {exp_type}",
            }


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


# Alias for compatibility
DataQualityFramework = SimpleDataQualityFramework
