# Data Quality Framework Integration Guide

This guide provides step-by-step instructions for integrating the Data Quality Framework into your data pipelines and applications.

## Quick Start

### 1. Basic Data Validation

```python
import pandas as pd
from libs.data_processing import DataQualityFramework, DataQualityConfig, create_common_expectations

# Sample data
df = pd.DataFrame({
    'user_id': [1, 2, 3, 4, 5],
    'email': ['user1@test.com', 'user2@test.com', None, 'user4@test.com', 'user5@test.com'],
    'age': [25, 30, 35, 40, 28]
})

# Initialize framework
config = DataQualityConfig()
dq_framework = DataQualityFramework(config)

# Create validation expectations
expectations = create_common_expectations(
    table_name="users",
    columns=["user_id", "email", "age"],
    primary_key_columns=["user_id"],
    not_null_columns=["user_id", "email"],
    unique_columns=["user_id"]
)

# Create expectation suite
suite_name = dq_framework.create_expectation_suite("users_validation", expectations)

# Validate data
result = dq_framework.validate_dataframe(df, suite_name, "users_table")

print(f"Validation Success: {result.success}")
print(f"Success Rate: {result.success_rate:.1f}%")
print(f"Failures: {result.failure_count}")
```

### 2. Data Profiling

```python
from libs.data_processing import DataProfiler

profiler = DataProfiler()
profile = profiler.profile_dataframe(df, "users_dataset")

print(f"Dataset: {profile.dataset_name}")
print(f"Rows: {profile.row_count}")
print(f"Missing Data: {profile.missing_percentage:.1f}%")
print(f"Warnings: {len(profile.warnings)}")

# Access column-specific statistics
for column_name, column_stats in profile.columns.items():
    print(f"\nColumn: {column_name}")
    print(f"Type: {column_stats['dtype']}")
    print(f"Missing: {column_stats['missing_count']}")
```

### 3. Data Lineage Tracking

```python
from libs.data_processing import DataLineageTracker

tracker = DataLineageTracker()

# Register source data
source_id = tracker.register_asset(
    name="raw_users.csv",
    asset_type="file",
    location="/data/raw/users.csv"
)

# Register target table
target_id = tracker.register_asset(
    name="users_table",
    asset_type="table",
    database="analytics_db",
    schema="public"
)

# Register transformation
transform_id = tracker.register_transformation(
    name="user_data_ingestion",
    transformation_type="etl",
    description="Ingest and validate user data from CSV"
)

# Create lineage relationship
tracker.add_lineage(source_id, target_id, transform_id)

# Analyze impact
impact = tracker.analyze_impact(source_id)
print(f"Downstream assets affected: {impact['total_downstream_assets']}")
```

## Integration Patterns

### 1. Data Pipeline Integration

```python
import asyncio
from services.data_ingestion.main import DataIngestionService, IngestionRequest, IngestionConfig

async def quality_aware_pipeline():
    service = DataIngestionService()
    
    # Configure quality checkpoints
    config = IngestionConfig(
        enable_validation=True,
        enable_profiling=True,
        enable_lineage=True,
        validation_suite="production_suite",
        fail_on_validation_error=True,  # Halt pipeline on quality issues
        max_validation_failures=5
    )
    
    request = IngestionRequest(
        dataset_name="customer_data",
        source_type="file",
        source_location="/data/raw/customers.csv",
        target_location="/data/processed/customers.parquet",
        validation_config=config
    )
    
    # Run ingestion with quality checks
    result = await service.ingest_data(request)
    
    if result.status == "success":
        print("✅ Data ingestion completed with quality validation")
        print(f"Records processed: {result.records_processed}")
        if result.validation_result:
            print(f"Validation success rate: {result.validation_result['success_rate']:.1f}%")
    else:
        print("❌ Data ingestion failed")
        for error in result.errors:
            print(f"Error: {error}")

# Run the pipeline
asyncio.run(quality_aware_pipeline())
```

### 2. API Monitoring Integration

```python
import requests
from datetime import datetime, timedelta

def monitor_data_quality():
    base_url = "http://localhost:8000/api/v1"
    headers = {"Authorization": "Bearer YOUR_TOKEN"}
    
    # Get overall quality metrics
    response = requests.get(f"{base_url}/data-quality/metrics", headers=headers)
    metrics = response.json()["data"]
    
    print(f"Overall Quality Score: {metrics['quality_score']:.1f}%")
    print(f"Validated Datasets: {metrics['validated_datasets']}/{metrics['total_datasets']}")
    print(f"Recent Failures: {metrics['recent_failures']}")
    
    # Get recent validation results
    response = requests.get(f"{base_url}/data-quality/validations?limit=5", headers=headers)
    validations = response.json()["data"]
    
    print("\nRecent Validations:")
    for validation in validations:
        status = "✅" if validation["success"] else "❌"
        print(f"{status} {validation['dataset_name']}: {validation['success_rate']:.1f}%")
    
    # Check for active alerts
    response = requests.get(f"{base_url}/data-quality/alerts?severity=high", headers=headers)
    alerts = response.json()["data"]
    
    if alerts:
        print(f"\n⚠️  {len(alerts)} high-severity alerts active:")
        for alert in alerts:
            print(f"- {alert['dataset']}: {alert['message']}")

monitor_data_quality()
```

### 3. Alerting System Setup

```python
from libs.data_processing import (
    DataQualityAlerting, 
    AlertingConfig, 
    AlertRule, 
    AlertSeverity, 
    AlertChannel,
    create_default_alert_rules
)

# Configure alerting system
config = AlertingConfig(
    enabled=True,
    default_channels=[AlertChannel.LOG, AlertChannel.EMAIL],
    email_smtp_host="smtp.company.com",
    email_smtp_port=587,
    email_from="data-quality@company.com",
    email_to=["data-team@company.com"]
)

alerting = DataQualityAlerting(config)

# Add default alert rules
for rule in create_default_alert_rules():
    alerting.add_rule(rule)

# Add custom alert rule
custom_rule = AlertRule(
    id="critical_failure_rate",
    name="Critical Failure Rate",
    description="Alert when validation failure rate exceeds 20%",
    severity=AlertSeverity.CRITICAL,
    channels=[AlertChannel.EMAIL, AlertChannel.SLACK],
    conditions={"max_failure_rate": 20.0},
    cooldown_minutes=30
)
alerting.add_rule(custom_rule)

# Evaluate validation results
validation_result = {
    "dataset_name": "critical_dataset",
    "success_rate": 75.0,  # Below threshold
    "failure_count": 25,
    "success": False
}

alerts = alerting.evaluate_validation_result(validation_result)
if alerts:
    alerting.send_alerts(alerts)
    print(f"Sent {len(alerts)} alerts for quality violations")
```

## Common Use Cases

### 1. Daily Data Quality Report

```python
from datetime import datetime, timedelta
import pandas as pd

def generate_daily_quality_report():
    """Generate a daily data quality summary report."""
    
    # Initialize components
    profiler = DataProfiler()
    tracker = DataLineageTracker()
    
    report_data = []
    
    # Example: Profile multiple datasets
    datasets = [
        ("sales_data", "/data/sales/daily_sales.csv"),
        ("user_events", "/data/events/user_events.parquet"),
        ("inventory", "/data/inventory/current_inventory.json")
    ]
    
    for dataset_name, file_path in datasets:
        try:
            # Load and profile data
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            else:
                continue
                
            profile = profiler.profile_dataframe(df, dataset_name)
            
            report_data.append({
                "dataset": dataset_name,
                "rows": profile.row_count,
                "columns": profile.column_count,
                "missing_pct": profile.missing_percentage,
                "warnings": len(profile.warnings),
                "quality_score": 100 - (profile.missing_percentage + len(profile.warnings) * 5)
            })
            
        except Exception as e:
            print(f"Error profiling {dataset_name}: {e}")
    
    # Create report
    report_df = pd.DataFrame(report_data)
    report_df.to_csv(f"quality_report_{datetime.now().strftime('%Y%m%d')}.csv", index=False)
    
    print("📊 Daily Quality Report Generated")
    print(report_df.to_string(index=False))
    
    return report_df

generate_daily_quality_report()
```

### 2. Data Pipeline Health Check

```python
def pipeline_health_check():
    """Check the health of data pipelines using lineage information."""
    
    tracker = DataLineageTracker()
    
    # Get all assets
    assets = tracker.graph.assets
    print(f"Total tracked assets: {len(assets)}")
    
    # Check for orphaned assets (no upstream or downstream)
    orphaned = []
    for asset_id, asset in assets.items():
        upstream = tracker.get_upstream_assets(asset_id)
        downstream = tracker.get_downstream_assets(asset_id)
        
        if not upstream and not downstream:
            orphaned.append(asset.name)
    
    if orphaned:
        print(f"⚠️  Orphaned assets found: {orphaned}")
    else:
        print("✅ All assets properly connected")
    
    # Check lineage integrity
    issues = tracker.validate_lineage_integrity()
    if issues:
        print(f"❌ Lineage integrity issues: {len(issues)}")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("✅ Lineage integrity validated")

pipeline_health_check()
```

### 3. Automated Quality Gates

```python
class DataQualityGate:
    """Automated quality gate for data pipeline promotion."""
    
    def __init__(self):
        self.config = DataQualityConfig()
        self.framework = DataQualityFramework(self.config)
        self.profiler = DataProfiler()
        
    def evaluate_dataset(self, df: pd.DataFrame, dataset_name: str) -> dict:
        """Evaluate if dataset meets quality standards for promotion."""
        
        results = {
            "dataset_name": dataset_name,
            "passed": False,
            "checks": {},
            "recommendations": []
        }
        
        # 1. Profile the data
        profile = self.profiler.profile_dataframe(df, dataset_name)
        
        # 2. Quality checks
        checks = {
            "row_count": profile.row_count >= 100,  # Minimum viable data
            "missing_data": profile.missing_percentage <= 10.0,  # Max 10% missing
            "duplicates": profile.duplicate_percentage <= 5.0,  # Max 5% duplicates
            "warnings": len(profile.warnings) <= 3  # Max 3 warnings
        }
        
        results["checks"] = checks
        results["passed"] = all(checks.values())
        
        # 3. Generate recommendations
        if not checks["missing_data"]:
            results["recommendations"].append("Investigate high missing data percentage")
        if not checks["duplicates"]:
            results["recommendations"].append("Remove duplicate records")
        if not checks["warnings"]:
            results["recommendations"].append("Address data quality warnings")
        
        return results
    
    def promote_if_ready(self, df: pd.DataFrame, dataset_name: str) -> bool:
        """Promote dataset to production if it passes quality gates."""
        
        evaluation = self.evaluate_dataset(df, dataset_name)
        
        if evaluation["passed"]:
            print(f"✅ {dataset_name} passes quality gates - PROMOTED")
            return True
        else:
            print(f"❌ {dataset_name} fails quality gates - BLOCKED")
            print("Failed checks:")
            for check, passed in evaluation["checks"].items():
                if not passed:
                    print(f"  - {check}")
            print("Recommendations:")
            for rec in evaluation["recommendations"]:
                print(f"  - {rec}")
            return False

# Usage
gate = DataQualityGate()
sample_df = pd.DataFrame({
    'id': range(1000),
    'value': [None if i % 20 == 0 else f"value_{i}" for i in range(1000)]
})

promoted = gate.promote_if_ready(sample_df, "production_dataset")
```

## Best Practices

### 1. Expectation Suite Management

```python
def create_dataset_specific_suite(dataset_name: str, df: pd.DataFrame):
    """Create tailored expectation suite for specific dataset."""
    
    # Analyze data characteristics
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    categorical_cols = df.select_dtypes(include=['object']).columns
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    
    expectations = []
    
    # Basic table expectations
    expectations.extend(create_common_expectations(
        table_name=dataset_name,
        columns=df.columns.tolist()
    ))
    
    # Numeric column expectations
    for col in numeric_cols:
        min_val, max_val = df[col].min(), df[col].max()
        expectations.append({
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": col, "min_value": min_val * 0.8, "max_value": max_val * 1.2}
        })
    
    # Categorical column expectations
    for col in categorical_cols:
        if df[col].nunique() < 20:  # Low cardinality
            values = df[col].dropna().unique().tolist()
            expectations.append({
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": col, "value_set": values}
            })
    
    return expectations
```

### 2. Performance Optimization

```python
def optimized_profiling(df: pd.DataFrame, dataset_name: str, sample_size: int = 10000):
    """Profile large datasets efficiently using sampling."""
    
    if len(df) > sample_size:
        sample_df = df.sample(n=sample_size, random_state=42)
        print(f"Using sample of {sample_size} rows from {len(df)} total rows")
    else:
        sample_df = df
    
    profiler = DataProfiler()
    profile = profiler.profile_dataframe(sample_df, dataset_name)
    
    # Scale statistics back to full dataset
    if len(df) > sample_size:
        scale_factor = len(df) / len(sample_df)
        profile.row_count = len(df)
        profile.missing_cells = int(profile.missing_cells * scale_factor)
        profile.duplicate_rows = int(profile.duplicate_rows * scale_factor)
    
    return profile
```

### 3. Error Handling and Resilience

```python
import logging
from typing import Optional

class RobustDataQuality:
    """Data quality framework with enhanced error handling."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def safe_validate(self, df: pd.DataFrame, suite_name: str, dataset_name: str) -> Optional[dict]:
        """Safely validate data with comprehensive error handling."""
        
        try:
            config = DataQualityConfig()
            framework = DataQualityFramework(config)
            
            result = framework.validate_dataframe(df, suite_name, dataset_name)
            return result.model_dump()
            
        except Exception as e:
            self.logger.error(f"Validation failed for {dataset_name}: {str(e)}")
            
            # Return degraded but functional result
            return {
                "success": False,
                "dataset_name": dataset_name,
                "error": str(e),
                "degraded_mode": True,
                "basic_stats": {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "has_nulls": df.isnull().any().any()
                }
            }
    
    def health_check(self) -> dict:
        """Check if all data quality components are functional."""
        
        checks = {}
        
        try:
            from libs.data_processing import DataQualityFramework
            checks["validation_framework"] = True
        except Exception as e:
            checks["validation_framework"] = False
            self.logger.error(f"Validation framework unavailable: {e}")
        
        try:
            from libs.data_processing import DataProfiler
            checks["profiler"] = True
        except Exception as e:
            checks["profiler"] = False
            self.logger.error(f"Profiler unavailable: {e}")
        
        try:
            from libs.data_processing import DataLineageTracker
            checks["lineage_tracker"] = True
        except Exception as e:
            checks["lineage_tracker"] = False
            self.logger.error(f"Lineage tracker unavailable: {e}")
        
        return {
            "overall_health": all(checks.values()),
            "component_status": checks,
            "degraded_components": [k for k, v in checks.items() if not v]
        }

# Usage
robust_dq = RobustDataQuality()
health = robust_dq.health_check()
print(f"Data Quality Health: {'✅ Healthy' if health['overall_health'] else '⚠️ Degraded'}")
```

## Troubleshooting Guide

### Common Issues and Solutions

1. **Import Errors**
   ```bash
   # Verify installation
   python -c "from libs.data_processing import DataQualityFramework; print('✅ Framework available')"
   
   # Reinstall dependencies
   uv sync
   ```

2. **Performance Issues**
   ```python
   # Use sampling for large datasets
   sample_df = df.sample(n=10000) if len(df) > 10000 else df
   
   # Profile with limited columns
   important_cols = ['id', 'timestamp', 'value']
   profile = profiler.profile_dataframe(df[important_cols], dataset_name)
   ```

3. **Memory Issues**
   ```python
   # Process data in chunks
   chunk_size = 5000
   for chunk in pd.read_csv('large_file.csv', chunksize=chunk_size):
       result = framework.validate_dataframe(chunk, suite_name, f"chunk_{i}")
   ```

This integration guide provides practical examples and patterns for implementing the Data Quality Framework in real-world scenarios. Refer to the main documentation (`docs/data_quality_framework.md`) for detailed API reference and configuration options.