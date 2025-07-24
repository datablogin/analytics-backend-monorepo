"""
Data Warehouse Connector Library

Provides unified interfaces for connecting to and querying enterprise data warehouses
including Snowflake, BigQuery, Redshift, and Azure Synapse Analytics.

Features:
- Unified connector interface for all warehouse types
- Async/await support for scalable operations
- OLAP operations (slice, dice, drill-down, roll-up)
- Query result caching and optimization
- Connection pooling for warehouse connections
- Schema discovery and metadata operations
- Federated query capabilities across multiple sources
"""

from .connectors.base import (
    ConnectionStatus,
    DataWarehouseConnector,
    QueryResult,
    SchemaInfo,
    WarehouseType,
)
from .connectors.factory import ConnectorFactory
from .olap.engine import OLAPEngine
from .query.cache import QueryCache
from .query.federation import FederatedQueryEngine

__all__ = [
    "DataWarehouseConnector",
    "QueryResult",
    "SchemaInfo",
    "ConnectionStatus",
    "WarehouseType",
    "ConnectorFactory",
    "OLAPEngine",
    "QueryCache",
    "FederatedQueryEngine",
]
