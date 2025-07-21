# Data Quality Framework

The Data Quality Framework provides comprehensive data validation, profiling, lineage tracking, and monitoring capabilities for the analytics platform. It ensures data integrity throughout the entire data pipeline with automated validation checkpoints and real-time quality monitoring.

## Overview

### Key Features

- **Automated Data Validation**: Rule-based validation using configurable expectations
- **Data Profiling**: Statistical analysis and quality assessment
- **Lineage Tracking**: Complete data asset and transformation tracking
- **Quality Monitoring**: Real-time dashboards and KPI tracking
- **Alerting System**: Multi-channel alerts for quality violations
- **Integration**: Seamless integration with data ingestion services

### Architecture

```
Data Quality Framework
├── Validation Engine (data_quality_simple.py)
├── Profiling Engine (profiling.py)
├── Lineage Tracker (lineage.py)
├── Alerting System (alerting.py)
├── API Endpoints (services/analytics_api/routes/data_quality.py)
└── Ingestion Integration (services/data_ingestion/main.py)
```

## Components

### 1. Data Quality Validation

#### Core Classes

- **`DataQualityFramework`**: Main validation engine
- **`DataQualityConfig`**: Configuration settings
- **`DataQualityResult`**: Validation results and metrics

#### Usage Example

```python
from libs.data_processing import DataQualityFramework, DataQualityConfig, create_common_expectations

# Initialize framework
config = DataQualityConfig()
dq_framework = DataQualityFramework(config)

# Create expectation suite
expectations = create_common_expectations(
    table_name="users",
    columns=["id", "name", "email"],
    primary_key_columns=["id"],
    not_null_columns=["id", "email"],
    unique_columns=["id", "email"]
)

suite_name = dq_framework.create_expectation_suite("users_suite", expectations)

# Validate DataFrame
result = dq_framework.validate_dataframe(df, suite_name, "users_table")
print(f"Validation success: {result.success}")
print(f"Success rate: {result.success_rate}%")
```

### 2. Data Profiling

#### Core Classes

- **`DataProfiler`**: Main profiling engine
- **`DataProfile`**: Profile results container
- **`ColumnProfile`**: Individual column statistics

#### Features

- Statistical analysis (mean, std, min, max, quartiles)
- Missing value detection
- Duplicate identification
- Data type inference
- Correlation analysis
- Quality warnings generation

#### Usage Example

```python
from libs.data_processing import DataProfiler

profiler = DataProfiler()
profile = profiler.profile_dataframe(df, "dataset_name")

print(f"Dataset: {profile.dataset_name}")
print(f"Rows: {profile.row_count}")
print(f"Columns: {profile.column_count}")
print(f"Missing: {profile.missing_percentage}%")
print(f"Warnings: {len(profile.warnings)}")
```

### 3. Data Lineage Tracking

#### Core Classes

- **`DataLineageTracker`**: Main lineage management
- **`DataAsset`**: Represents data assets (tables, files, etc.)
- **`DataTransformation`**: Represents ETL processes
- **`LineageEdge`**: Represents relationships between assets

#### Features

- Asset registration and tracking
- Transformation documentation
- Upstream/downstream impact analysis
- Circular dependency detection
- Lineage integrity validation

#### Usage Example

```python
from libs.data_processing import DataLineageTracker

tracker = DataLineageTracker()

# Register assets
source_id = tracker.register_asset("raw_data.csv", "file", location="/data/raw/")
target_id = tracker.register_asset("clean_data", "table", database="analytics")

# Register transformation
transform_id = tracker.register_transformation(
    "data_cleaning", 
    "etl", 
    description="Clean and validate raw data"
)

# Add lineage relationship
tracker.add_lineage(source_id, target_id, transform_id)

# Analyze impact
impact = tracker.analyze_impact(source_id)
print(f"Downstream assets: {impact['total_downstream_assets']}")
```

### 4. Alerting System

#### Core Classes

- **`DataQualityAlerting`**: Main alerting engine
- **`AlertRule`**: Configurable alert conditions
- **`Alert`**: Alert instances and metadata
- **`AlertingConfig`**: System configuration

#### Alert Channels

- **LOG**: Structured logging alerts
- **EMAIL**: Email notifications (configuration required)
- **SLACK**: Slack webhooks (configuration required)
- **WEBHOOK**: Generic HTTP webhooks

#### Usage Example

```python
from libs.data_processing import DataQualityAlerting, AlertingConfig, create_default_alert_rules

# Initialize alerting
config = AlertingConfig(enabled=True, default_channels=["LOG"])
alerting = DataQualityAlerting(config)

# Add default rules
for rule in create_default_alert_rules():
    alerting.add_rule(rule)

# Evaluate validation results
alerts = alerting.evaluate_validation_result(validation_result.model_dump())
if alerts:
    alerting.send_alerts(alerts)
```

## API Endpoints

The framework provides REST API endpoints for monitoring and management:

### Quality Metrics
- **GET `/data-quality/metrics`**: Overall quality KPIs
- **GET `/data-quality/validations`**: Recent validation results
- **POST `/data-quality/validate`**: Trigger dataset validation

### Data Profiling
- **GET `/data-quality/profiles`**: Recent profiling results

### Lineage Management
- **GET `/data-quality/lineage/{asset_name}`**: Asset lineage information
- **GET `/data-quality/lineage/{asset_name}/impact`**: Impact analysis
- **GET `/data-quality/lineage/graph`**: Complete lineage graph

### Alerting
- **GET `/data-quality/alerts`**: Recent quality alerts
- **GET `/data-quality/suites`**: Available expectation suites

## Integration with Data Ingestion

The framework integrates seamlessly with the data ingestion service (`services/data_ingestion/main.py`):

### Ingestion Pipeline Checkpoints

1. **Pre-ingestion Validation**: Validate raw data before processing
2. **Data Profiling**: Generate statistical profiles during ingestion
3. **Lineage Registration**: Automatically track data flow
4. **Post-ingestion Alerts**: Send notifications for quality issues

### Configuration Options

```python
from services.data_ingestion.main import IngestionConfig

config = IngestionConfig(
    enable_validation=True,
    enable_profiling=True,
    enable_lineage=True,
    validation_suite="default_suite",
    fail_on_validation_error=False,
    max_validation_failures=10
)
```

## Configuration

### Environment Variables

```bash
# Data Quality Settings
DQ_ENABLE_VALIDATION=true
DQ_ENABLE_PROFILING=true
DQ_ENABLE_LINEAGE=true
DQ_CONTEXT_ROOT_DIR=/opt/data_quality/great_expectations

# Alerting Configuration
ALERT_ENABLE=true
ALERT_EMAIL_SMTP_HOST=smtp.company.com
ALERT_EMAIL_SMTP_PORT=587
ALERT_SLACK_WEBHOOK_URL=https://hooks.slack.com/...
```

### Framework Configuration

```python
from libs.data_processing import DataQualityConfig, AlertingConfig

# Validation configuration
dq_config = DataQualityConfig(
    context_root_dir="great_expectations",
    datasource_name="analytics_datasource",
    expectation_suite_name="default_suite"
)

# Alerting configuration
alert_config = AlertingConfig(
    enabled=True,
    default_channels=["LOG", "EMAIL"],
    cooldown_minutes=60
)
```

## Testing

The framework includes comprehensive tests in `tests/test_data_quality_framework.py`:

```bash
# Run data quality tests
make test -k test_data_quality

# Run specific test classes
pytest tests/test_data_quality_framework.py::TestDataQualityFramework
pytest tests/test_data_quality_framework.py::TestDataProfiler
pytest tests/test_data_quality_framework.py::TestDataLineageTracker
pytest tests/test_data_quality_framework.py::TestDataQualityAlerting
```

## Monitoring and KPIs

### Key Performance Indicators

- **Data Quality Score**: Overall quality percentage across all datasets
- **Validation Success Rate**: Percentage of successful validations (Target: 99.5%)
- **Coverage**: Percentage of datasets with quality monitoring
- **Detection Time**: Average time to detect quality issues (Target: <15 minutes)
- **Alert Response Time**: Time from detection to notification

### Dashboard Metrics

Access quality metrics through the API:

```bash
# Get overall metrics
curl -X GET /api/v1/data-quality/metrics

# Get recent validations
curl -X GET /api/v1/data-quality/validations?limit=20

# Get quality alerts
curl -X GET /api/v1/data-quality/alerts?severity=high
```

## Troubleshooting

### Common Issues

1. **Validation Failures**
   - Check expectation suite configuration
   - Verify data schema matches expectations
   - Review validation logs for specific failures

2. **Performance Issues**
   - Limit profiling to sample data for large datasets
   - Use async processing for validation tasks
   - Consider caching frequently accessed profiles

3. **Alert Fatigue**
   - Adjust alert rule thresholds
   - Configure appropriate cooldown periods
   - Use alert severity levels effectively

### Debug Commands

```bash
# Check data quality framework status
python -c "from libs.data_processing import DataQualityFramework; print('Framework available')"

# Test validation on sample data
python -m libs.data_processing.data_quality_simple

# Check lineage tracker
python -c "from libs.data_processing import DataLineageTracker; t = DataLineageTracker(); print('Lineage tracker ready')"
```

## Best Practices

### Expectation Management
- Create specific suites for each dataset type
- Use business rules to define quality expectations
- Regularly review and update expectations
- Version control expectation suites

### Performance Optimization
- Use sampling for large dataset profiling
- Implement caching for frequently accessed data
- Schedule heavy operations during off-peak hours
- Monitor resource usage and optimize accordingly

### Alert Management
- Set appropriate severity levels
- Configure realistic thresholds
- Use cooldown periods to prevent spam
- Establish clear escalation procedures

### Lineage Tracking
- Document all transformations thoroughly
- Use consistent naming conventions
- Register assets as early as possible
- Regularly validate lineage integrity

## Future Enhancements

### Planned Features
- Machine learning-based anomaly detection
- Data quality scoring algorithms
- Advanced drift detection
- Integration with external data catalogs
- Real-time streaming validation
- Enhanced visualization dashboards

### Integration Roadmap
- Apache Airflow integration for pipeline orchestration
- Kafka integration for real-time quality monitoring
- Data catalog integration (Apache Atlas, DataHub)
- BI tool integration (Tableau, Power BI)