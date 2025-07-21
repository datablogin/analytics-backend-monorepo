"""Data profiling and statistical analysis utilities."""

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import structlog
from pydantic import BaseModel, Field
from ydata_profiling import ProfileReport

logger = structlog.get_logger(__name__)


class DataProfile(BaseModel):
    """Data profiling results."""

    dataset_name: str
    profiling_time: datetime
    row_count: int
    column_count: int
    memory_usage: int
    missing_cells: int
    missing_cells_percentage: float
    duplicate_rows: int
    duplicate_rows_percentage: float
    columns: dict[str, dict[str, Any]] = Field(default_factory=dict)
    correlations: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class ColumnProfile(BaseModel):
    """Individual column profiling results."""

    name: str
    dtype: str
    missing_count: int
    missing_percentage: float
    unique_count: int
    unique_percentage: float
    most_frequent_value: Any | None = None
    most_frequent_count: int | None = None
    statistics: dict[str, Any] = Field(default_factory=dict)


class DataProfiler:
    """Data profiling and analysis tool."""

    def __init__(self):
        self.logger = structlog.get_logger(__name__)

    def profile_dataframe(
        self, df: pd.DataFrame, dataset_name: str, minimal: bool = False
    ) -> DataProfile:
        """Generate comprehensive data profile for a DataFrame."""
        try:
            profiling_time = datetime.utcnow()

            # Basic statistics
            row_count = len(df)
            column_count = len(df.columns)
            memory_usage = df.memory_usage(deep=True).sum()

            # Missing data analysis
            missing_cells = df.isnull().sum().sum()
            total_cells = row_count * column_count
            missing_cells_percentage = (
                missing_cells / total_cells * 100 if total_cells > 0 else 0
            )

            # Duplicate analysis
            duplicate_rows = df.duplicated().sum()
            duplicate_rows_percentage = (
                duplicate_rows / row_count * 100 if row_count > 0 else 0
            )

            # Column profiling
            columns = {}
            for col in df.columns:
                columns[col] = self._profile_column(df[col], col)

            # Correlations (numeric columns only)
            correlations = {}
            numeric_df = df.select_dtypes(include=[np.number])
            if len(numeric_df.columns) > 1:
                corr_matrix = numeric_df.corr()
                correlations = {
                    "method": "pearson",
                    "matrix": corr_matrix.to_dict(),
                    "high_correlations": self._find_high_correlations(corr_matrix),
                }

            # Generate warnings
            warnings = self._generate_warnings(
                df, missing_cells_percentage, duplicate_rows_percentage
            )

            profile = DataProfile(
                dataset_name=dataset_name,
                profiling_time=profiling_time,
                row_count=row_count,
                column_count=column_count,
                memory_usage=memory_usage,
                missing_cells=missing_cells,
                missing_cells_percentage=missing_cells_percentage,
                duplicate_rows=duplicate_rows,
                duplicate_rows_percentage=duplicate_rows_percentage,
                columns=columns,
                correlations=correlations,
                warnings=warnings,
            )

            self.logger.info(
                "Data profiling completed",
                dataset_name=dataset_name,
                row_count=row_count,
                column_count=column_count,
                warnings_count=len(warnings),
            )

            return profile

        except Exception as e:
            self.logger.error(
                "Data profiling failed", dataset_name=dataset_name, error=str(e)
            )
            raise

    def _profile_column(self, series: pd.Series, column_name: str) -> dict[str, Any]:
        """Profile individual column."""
        missing_count = series.isnull().sum()
        missing_percentage = missing_count / len(series) * 100
        unique_count = series.nunique()
        unique_percentage = unique_count / len(series) * 100

        # Most frequent value
        if len(series) > 0 and not series.isnull().all():
            value_counts = series.value_counts()
            most_frequent_value = (
                value_counts.index[0] if len(value_counts) > 0 else None
            )
            most_frequent_count = (
                value_counts.iloc[0] if len(value_counts) > 0 else None
            )
        else:
            most_frequent_value = None
            most_frequent_count = None

        # Type-specific statistics
        statistics = {}
        if pd.api.types.is_bool_dtype(series):
            # Boolean columns should be treated as categorical, not numeric
            statistics.update(self._categorical_statistics(series))
        elif pd.api.types.is_numeric_dtype(series):
            statistics.update(self._numeric_statistics(series))
        elif pd.api.types.is_datetime64_any_dtype(series):
            statistics.update(self._datetime_statistics(series))
        else:
            statistics.update(self._categorical_statistics(series))

        return {
            "name": column_name,
            "dtype": str(series.dtype),
            "missing_count": int(missing_count),
            "missing_percentage": float(missing_percentage),
            "unique_count": int(unique_count),
            "unique_percentage": float(unique_percentage),
            "most_frequent_value": most_frequent_value,
            "most_frequent_count": int(most_frequent_count)
            if most_frequent_count
            else None,
            "statistics": statistics,
        }

    def _numeric_statistics(self, series: pd.Series) -> dict[str, Any]:
        """Calculate statistics for numeric columns."""
        if series.isnull().all():
            return {}

        stats = {
            "min": float(series.min()),
            "max": float(series.max()),
            "mean": float(series.mean()),
            "median": float(series.median()),
            "std": float(series.std()),
            "variance": float(series.var()),
            "skewness": float(series.skew()),
            "kurtosis": float(series.kurtosis()),
            "q1": float(series.quantile(0.25)),
            "q3": float(series.quantile(0.75)),
            "iqr": float(series.quantile(0.75) - series.quantile(0.25)),
            "zeros_count": int((series == 0).sum()),
            "zeros_percentage": float((series == 0).sum() / len(series) * 100),
            "negative_count": int((series < 0).sum()),
            "negative_percentage": float((series < 0).sum() / len(series) * 100),
        }

        # Outlier detection using IQR method
        q1, q3 = stats["q1"], stats["q3"]
        iqr = stats["iqr"]
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = series[(series < lower_bound) | (series > upper_bound)]
        stats["outliers_count"] = len(outliers)
        stats["outliers_percentage"] = float(len(outliers) / len(series) * 100)

        return stats

    def _datetime_statistics(self, series: pd.Series) -> dict[str, Any]:
        """Calculate statistics for datetime columns."""
        if series.isnull().all():
            return {}

        return {
            "min": series.min().isoformat() if pd.notna(series.min()) else None,
            "max": series.max().isoformat() if pd.notna(series.max()) else None,
            "range_days": (series.max() - series.min()).days
            if pd.notna(series.min()) and pd.notna(series.max())
            else None,
        }

    def _categorical_statistics(self, series: pd.Series) -> dict[str, Any]:
        """Calculate statistics for categorical columns."""
        if series.isnull().all():
            return {}

        stats = {
            "max_length": int(series.astype(str).str.len().max()),
            "min_length": int(series.astype(str).str.len().min()),
            "avg_length": float(series.astype(str).str.len().mean()),
        }

        # Top categories
        value_counts = series.value_counts().head(10)
        stats["top_categories"] = [
            {"value": str(val), "count": int(count)}
            for val, count in value_counts.items()
        ]

        return stats

    def _find_high_correlations(
        self, corr_matrix: pd.DataFrame, threshold: float = 0.8
    ) -> list[dict[str, Any]]:
        """Find highly correlated column pairs."""
        high_corr = []

        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) >= threshold:
                    high_corr.append(
                        {
                            "column1": corr_matrix.columns[i],
                            "column2": corr_matrix.columns[j],
                            "correlation": float(corr_value),
                        }
                    )

        return sorted(high_corr, key=lambda x: abs(x["correlation"]), reverse=True)

    def _generate_warnings(
        self, df: pd.DataFrame, missing_percentage: float, duplicate_percentage: float
    ) -> list[str]:
        """Generate data quality warnings."""
        warnings = []

        # High missing data
        if missing_percentage > 2:
            warnings.append(
                f"High missing data: {missing_percentage:.1f}% of cells are empty"
            )

        # High duplicate rate
        if duplicate_percentage > 5:
            warnings.append(
                f"High duplicate rate: {duplicate_percentage:.1f}% of rows are duplicated"
            )

        # Columns with all missing values
        all_null_cols = [col for col in df.columns if df[col].isnull().all()]
        if all_null_cols:
            warnings.append(
                f"Columns with all missing values: {', '.join(all_null_cols)}"
            )

        # Columns with single unique value
        single_value_cols = [col for col in df.columns if df[col].nunique() == 1]
        if single_value_cols:
            warnings.append(
                f"Columns with single unique value: {', '.join(single_value_cols)}"
            )

        # Very high cardinality columns
        high_cardinality_cols = [
            col
            for col in df.columns
            if df[col].nunique() > 0.9 * len(df) and len(df) > 100
        ]
        if high_cardinality_cols:
            warnings.append(
                f"High cardinality columns: {', '.join(high_cardinality_cols)}"
            )

        return warnings

    def generate_html_report(
        self,
        df: pd.DataFrame,
        title: str = "Data Profile Report",
        output_file: str | None = None,
    ) -> str:
        """Generate detailed HTML profiling report using pandas-profiling."""
        try:
            profile = ProfileReport(df, title=title, explorative=True, minimal=False)

            if output_file:
                profile.to_file(output_file)
                self.logger.info("HTML report saved", output_file=output_file)
                return output_file
            else:
                html_content = profile.to_html()
                return html_content

        except Exception as e:
            self.logger.error("Failed to generate HTML report", error=str(e))
            raise

    def compare_profiles(
        self, profile1: DataProfile, profile2: DataProfile
    ) -> dict[str, Any]:
        """Compare two data profiles to detect drift."""
        column_changes: dict[str, dict[str, float]] = {}
        warnings: list[str] = []

        comparison = {
            "dataset1": profile1.dataset_name,
            "dataset2": profile2.dataset_name,
            "row_count_change": profile2.row_count - profile1.row_count,
            "column_count_change": profile2.column_count - profile1.column_count,
            "missing_percentage_change": profile2.missing_cells_percentage
            - profile1.missing_cells_percentage,
            "duplicate_percentage_change": profile2.duplicate_rows_percentage
            - profile1.duplicate_rows_percentage,
            "column_changes": column_changes,
            "warnings": warnings,
        }

        # Compare common columns
        common_columns = set(profile1.columns.keys()) & set(profile2.columns.keys())
        for col in common_columns:
            col1 = profile1.columns[col]
            col2 = profile2.columns[col]

            column_changes[col] = {
                "missing_percentage_change": col2["missing_percentage"]
                - col1["missing_percentage"],
                "unique_percentage_change": col2["unique_percentage"]
                - col1["unique_percentage"],
            }

            # Check for significant changes
            if abs(col2["missing_percentage"] - col1["missing_percentage"]) > 5:
                warnings.append(
                    f"Column {col}: missing data changed by {col2['missing_percentage'] - col1['missing_percentage']:.1f}%"
                )

        # Check for new/removed columns
        new_columns = set(profile2.columns.keys()) - set(profile1.columns.keys())
        removed_columns = set(profile1.columns.keys()) - set(profile2.columns.keys())

        if new_columns:
            warnings.append(f"New columns detected: {', '.join(new_columns)}")
        if removed_columns:
            warnings.append(f"Columns removed: {', '.join(removed_columns)}")

        return comparison
