"""
Data warehouse API endpoints.

This module provides REST API endpoints for data warehouse operations
including connection management, querying, and OLAP operations.
"""

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel

from libs.analytics_core.auth import require_permissions
from libs.analytics_core.models import User
from libs.analytics_core.responses import StandardResponse
from libs.data_warehouse import (
    WarehouseType,
)
from libs.data_warehouse.connectors.base import ConnectionStatus, QueryResult
from libs.data_warehouse.connectors.factory import ConnectorFactory
from libs.data_warehouse.dependencies import (
    DataWarehouseManager,
    get_data_warehouse_manager,
)
from libs.data_warehouse.olap.cube import CubeQuery, CubeResult, CubeSchema
from libs.data_warehouse.olap.operations import (
    DiceOperation,
    DrillDownOperation,
    DrillUpOperation,
    PivotOperation,
    SliceOperation,
)

router = APIRouter(prefix="/data-warehouse", tags=["Data Warehouse"])


# Request/Response Models
class ConnectionRequest(BaseModel):
    """Request to create a data warehouse connection."""

    name: str
    warehouse_type: WarehouseType
    connection_params: dict[str, Any]
    description: str | None = None


class ConnectionResponse(BaseModel):
    """Response for connection operations."""

    name: str
    warehouse_type: WarehouseType
    status: ConnectionStatus
    description: str | None = None


class QueryRequest(BaseModel):
    """Request to execute a query."""

    query: str
    params: dict[str, Any] | None = None
    timeout: int | None = None
    use_cache: bool = True


class CubeRegistrationRequest(BaseModel):
    """Request to register an OLAP cube."""

    cube_schema: CubeSchema


class OLAPQueryRequest(BaseModel):
    """Request for OLAP query operations."""

    cube_name: str
    dimensions: list[str] = []
    measures: list[str] = []
    filters: dict[str, Any] = {}
    having: dict[str, Any] = {}
    order_by: list[str] = []
    limit: int | None = None


class OLAPOperationRequest(BaseModel):
    """Request for OLAP operations."""

    cube_name: str
    operation_type: str  # slice, dice, drill_down, drill_up, pivot
    operation_params: dict[str, Any]
    base_query: CubeQuery | None = None


class FederatedQueryRequest(BaseModel):
    """Request for federated queries."""

    name: str
    sources: list[str]
    queries: dict[str, str]
    join_strategy: str = "union"
    join_keys: list[str] | None = None
    aggregation: dict[str, str] | None = None


# Dependencies are now injected through DataWarehouseManager


@router.post("/connections", response_model=StandardResponse[ConnectionResponse])
async def create_connection(
    request: ConnectionRequest,
    current_user: User = Depends(
        require_permissions("data_warehouse:connection:create")
    ),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[ConnectionResponse]:
    """Create a new data warehouse connection."""
    try:
        # Create connection pool through manager
        pool = await manager.create_connection_pool(
            request.name, request.warehouse_type.value, request.connection_params
        )

        # Test pool health
        async with pool.acquire() as connector:
            await connector.test_connection()

            # Register with federation engine using proxy
            federation_engine = manager.get_federation_engine()
            from libs.data_warehouse.connectors.pool_proxy import PoolProxyConnector

            proxy_connector = PoolProxyConnector(pool)
            federation_engine.register_source(request.name, proxy_connector)

            response_data = ConnectionResponse(
                name=request.name,
                warehouse_type=request.warehouse_type,
                status=proxy_connector.status,
                description=request.description,
            )

        return StandardResponse(
            success=True,
            data=response_data,
            message=f"Connection '{request.name}' created successfully",
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to create connection: {str(e)}"
        )


@router.get("/connections", response_model=StandardResponse[list[ConnectionResponse]])
async def list_connections(
    current_user: User = Depends(require_permissions("data_warehouse:connection:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[list[ConnectionResponse]]:
    """List all data warehouse connections."""
    connections = []
    pool_stats = manager.get_pool_stats()

    for pool_id, stats in pool_stats.items():
        # Determine status based on pool health
        status = (
            ConnectionStatus.CONNECTED
            if stats["total_connections"] > 0
            else ConnectionStatus.DISCONNECTED
        )

        connections.append(
            ConnectionResponse(
                name=pool_id,
                warehouse_type=WarehouseType(stats["warehouse_type"]),
                status=status,
                description=None,  # Could be enhanced to store descriptions
            )
        )

    return StandardResponse(
        success=True, data=connections, message=f"Found {len(connections)} connections"
    )


@router.get("/connections/{connection_name}/status")
async def get_connection_status(
    connection_name: str,
    current_user: User = Depends(require_permissions("data_warehouse:connection:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[dict[str, Any]]:
    """Get status of a specific connection."""
    pool = await manager.get_connection_pool(connection_name)
    if not pool:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        # Test pool health
        async with pool.acquire() as connector:
            is_healthy = await connector.test_connection()

        pool_stats = pool.get_stats()
        status_info = {
            "name": connection_name,
            "status": "connected"
            if pool_stats["total_connections"] > 0
            else "disconnected",
            "warehouse_type": pool.warehouse_type.value,
            "healthy": is_healthy,
            "pool_stats": pool_stats,
        }

        return StandardResponse(
            success=True, data=status_info, message="Connection status retrieved"
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to check connection status: {str(e)}"
        )


@router.post("/connections/{connection_name}/query")
async def execute_query(
    connection_name: str,
    request: QueryRequest,
    current_user: User = Depends(require_permissions("data_warehouse:query:execute")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[QueryResult]:
    """Execute a SQL query on a data warehouse."""
    pool = await manager.get_connection_pool(connection_name)
    if not pool:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        # Check cache first if enabled
        query_cache = manager.get_query_cache()
        if request.use_cache:
            cached_result = query_cache.get(request.query, request.params)
            if cached_result:
                return StandardResponse(
                    success=True, data=cached_result, message="Query result from cache"
                )

        # Execute query using pool
        async with pool.acquire() as connector:
            result = await connector.execute_query(
                request.query, request.params, request.timeout
            )

        # Cache result if caching is enabled
        if request.use_cache:
            query_cache.put(request.query, result, request.params)

        return StandardResponse(
            success=True, data=result, message="Query executed successfully"
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")


@router.post("/connections/{connection_name}/query/async")
async def execute_async_query(
    connection_name: str,
    request: QueryRequest,
    current_user: User = Depends(require_permissions("data_warehouse:query:execute")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[dict[str, str]]:
    """Execute a query asynchronously and return query ID."""
    pool = await manager.get_connection_pool(connection_name)
    if not pool:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        async with pool.acquire() as connector:
            query_id = await connector.execute_async_query(
                request.query, request.params
            )

        return StandardResponse(
            success=True, data={"query_id": query_id}, message="Async query started"
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Async query execution failed: {str(e)}"
        )


@router.get("/connections/{connection_name}/query/{query_id}/status")
async def get_query_status(
    connection_name: str,
    query_id: str,
    current_user: User = Depends(require_permissions("data_warehouse:query:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[dict[str, Any]]:
    """Get status of an async query."""
    pool = await manager.get_connection_pool(connection_name)
    if not pool:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        async with pool.acquire() as connector:
            status = await connector.get_query_status(query_id)

        return StandardResponse(
            success=True, data=status, message="Query status retrieved"
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to get query status: {str(e)}"
        )


@router.get("/connections/{connection_name}/query/{query_id}/result")
async def get_query_result(
    connection_name: str,
    query_id: str,
    current_user: User = Depends(require_permissions("data_warehouse:query:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[QueryResult]:
    """Get result of a completed async query."""
    pool = await manager.get_connection_pool(connection_name)
    if not pool:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        async with pool.acquire() as connector:
            result = await connector.get_query_result(query_id)

        return StandardResponse(
            success=True, data=result, message="Query result retrieved"
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to get query result: {str(e)}"
        )


@router.get("/connections/{connection_name}/schema")
async def get_schema_info(
    connection_name: str,
    schema_name: str | None = Query(None),
    current_user: User = Depends(require_permissions("data_warehouse:schema:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[Any]:
    """Get schema information from a data warehouse."""
    pool = await manager.get_connection_pool(connection_name)
    if not pool:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        async with pool.acquire() as connector:
            schema_info = await connector.get_schema_info(schema_name)

        return StandardResponse(
            success=True, data=schema_info, message="Schema information retrieved"
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to get schema info: {str(e)}"
        )


# OLAP Endpoints
@router.post("/olap/cubes", response_model=StandardResponse[dict[str, str]])
async def register_cube(
    request: CubeRegistrationRequest,
    connection_name: str = Query(..., description="Connection to register cube with"),
    current_user: User = Depends(require_permissions("data_warehouse:olap:create")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[dict[str, str]]:
    """Register an OLAP cube for multidimensional analysis."""
    try:
        from libs.data_warehouse.olap.cube import DataCube

        engine = await manager.get_olap_engine(connection_name)
        if not engine:
            raise HTTPException(
                status_code=404, detail="OLAP engine not found for connection"
            )

        cube = DataCube(request.cube_schema)
        engine.register_cube(cube)

        return StandardResponse(
            success=True,
            data={"cube_name": request.cube_schema.name},
            message=f"Cube '{request.cube_schema.name}' registered successfully",
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to register cube: {str(e)}"
        )


@router.get("/olap/cubes")
async def list_cubes(
    connection_name: str = Query(...),
    current_user: User = Depends(require_permissions("data_warehouse:olap:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[list[str]]:
    """List all registered OLAP cubes for a connection."""
    engine = await manager.get_olap_engine(connection_name)
    if not engine:
        raise HTTPException(
            status_code=404, detail="OLAP engine not found for connection"
        )

    cubes = engine.list_cubes()

    return StandardResponse(
        success=True, data=cubes, message=f"Found {len(cubes)} cubes"
    )


@router.get("/olap/cubes/{cube_name}/metadata")
async def get_cube_metadata(
    cube_name: str,
    connection_name: str = Query(...),
    current_user: User = Depends(require_permissions("data_warehouse:olap:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[dict[str, Any]]:
    """Get metadata about an OLAP cube."""
    try:
        engine = await manager.get_olap_engine(connection_name)
        if not engine:
            raise HTTPException(
                status_code=404, detail="OLAP engine not found for connection"
            )

        metadata = await engine.get_cube_metadata(cube_name)

        return StandardResponse(
            success=True, data=metadata, message="Cube metadata retrieved"
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to get cube metadata: {str(e)}"
        )


@router.post("/olap/query")
async def execute_olap_query(
    request: OLAPQueryRequest,
    connection_name: str = Query(...),
    current_user: User = Depends(require_permissions("data_warehouse:olap:query")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[CubeResult]:
    """Execute an OLAP query on a cube."""
    try:
        engine = await manager.get_olap_engine(connection_name)
        if not engine:
            raise HTTPException(
                status_code=404, detail="OLAP engine not found for connection"
            )

        # Create cube query
        cube_query = CubeQuery(
            cube_name=request.cube_name,
            dimensions=request.dimensions,
            measures=request.measures,
            filters=request.filters,
            having=request.having,
            order_by=request.order_by,
            limit=request.limit,
        )

        # Execute query
        result = await engine.execute_query(cube_query)

        return StandardResponse(
            success=True, data=result, message="OLAP query executed successfully"
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"OLAP query execution failed: {str(e)}"
        )


@router.post("/olap/operations")
async def execute_olap_operation(
    request: OLAPOperationRequest,
    connection_name: str = Query(...),
    current_user: User = Depends(require_permissions("data_warehouse:olap:query")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[CubeResult]:
    """Execute an OLAP operation (slice, dice, drill-down, etc.)."""
    try:
        engine = await manager.get_olap_engine(connection_name)
        if not engine:
            raise HTTPException(
                status_code=404, detail="OLAP engine not found for connection"
            )

        # Create operation based on type
        operation = None
        op_type = request.operation_type.lower()
        params = request.operation_params

        if op_type == "slice":
            operation = SliceOperation(params["dimension"], params["value"])
        elif op_type == "dice":
            operation = DiceOperation(params["filters"])
        elif op_type == "drill_down":
            operation = DrillDownOperation(
                params["dimension"], params.get("from_level"), params.get("to_level")
            )
        elif op_type == "drill_up":
            operation = DrillUpOperation(
                params["dimension"], params.get("from_level"), params.get("to_level")
            )
        elif op_type == "pivot":
            operation = PivotOperation(
                params["new_row_dimensions"], params.get("new_column_dimensions")
            )
        else:
            raise ValueError(f"Unsupported operation type: {request.operation_type}")

        # Execute operation
        result = await engine.execute_operation(
            request.cube_name, operation, request.base_query
        )

        return StandardResponse(
            success=True,
            data=result,
            message=f"OLAP operation '{op_type}' executed successfully",
        )

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"OLAP operation failed: {str(e)}")


# Federated Query Endpoints
@router.post("/federated/queries")
async def register_federated_query(
    request: FederatedQueryRequest,
    current_user: User = Depends(
        require_permissions("data_warehouse:federation:create")
    ),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[dict[str, str]]:
    """Register a federated query across multiple data sources."""
    try:
        from libs.data_warehouse.query.federation import FederatedQuery

        federated_query = FederatedQuery(
            name=request.name,
            sources=request.sources,
            queries=request.queries,
            join_strategy=request.join_strategy,
            join_keys=request.join_keys,
            aggregation=request.aggregation,
        )

        federation_engine = manager.get_federation_engine()
        federation_engine.register_query(federated_query)

        return StandardResponse(
            success=True,
            data={"query_name": request.name},
            message=f"Federated query '{request.name}' registered successfully",
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to register federated query: {str(e)}"
        )


@router.post("/federated/queries/{query_name}/execute")
async def execute_federated_query(
    query_name: str,
    params: dict[str, dict[str, Any]] | None = Body(None),
    current_user: User = Depends(
        require_permissions("data_warehouse:federation:execute")
    ),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[Any]:
    """Execute a registered federated query."""
    try:
        federation_engine = manager.get_federation_engine()
        result = await federation_engine.execute_federated_query(query_name, params)

        return StandardResponse(
            success=result.success,
            data=result,
            message=f"Federated query '{query_name}' executed",
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Federated query execution failed: {str(e)}"
        )


@router.get("/federated/sources")
async def list_federation_sources(
    current_user: User = Depends(require_permissions("data_warehouse:federation:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[list[str]]:
    """List all data sources available for federation."""
    federation_engine = manager.get_federation_engine()
    sources = federation_engine.get_sources()

    return StandardResponse(
        success=True, data=sources, message=f"Found {len(sources)} federation sources"
    )


# Cache Management Endpoints
@router.get("/cache/stats")
async def get_cache_stats(
    current_user: User = Depends(require_permissions("data_warehouse:cache:read")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[dict[str, Any]]:
    """Get query cache statistics."""
    query_cache = manager.get_query_cache()
    stats = query_cache.get_stats()

    return StandardResponse(
        success=True, data=stats, message="Cache statistics retrieved"
    )


@router.post("/cache/clear")
async def clear_cache(
    table_pattern: str | None = Body(
        None, description="Optional table pattern to match"
    ),
    current_user: User = Depends(require_permissions("data_warehouse:cache:manage")),
    manager: DataWarehouseManager = Depends(get_data_warehouse_manager),
) -> StandardResponse[dict[str, Any]]:
    """Clear query cache entries."""
    query_cache = manager.get_query_cache()
    if table_pattern:
        cleared = query_cache.invalidate_pattern(table_pattern)
        message = f"Cleared {cleared} cache entries matching pattern '{table_pattern}'"
    else:
        query_cache.clear()
        cleared = "all"
        message = "Cleared all cache entries"

    return StandardResponse(
        success=True, data={"cleared_entries": cleared}, message=message
    )


# Configuration Management Endpoints
@router.get("/config/schemas")
async def get_supported_warehouse_types(
    current_user: User = Depends(require_permissions("data_warehouse:config:read")),
) -> StandardResponse[dict[str, Any]]:
    """Get list of supported warehouse types and their configuration schemas."""
    try:
        supported_types = ConnectorFactory.get_supported_types()

        schemas = {}
        for warehouse_type in supported_types:
            try:
                schema = ConnectorFactory.get_config_schema(warehouse_type)
                schemas[warehouse_type.value] = {
                    "type": warehouse_type.value,
                    "name": warehouse_type.value.title(),
                    "schema": schema,
                }
            except Exception:
                # Log error but continue with other types
                pass

        return StandardResponse(
            success=True,
            data={
                "supported_types": [wt.value for wt in supported_types],
                "schemas": schemas,
            },
            message=f"Found {len(supported_types)} supported warehouse types",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get warehouse type schemas: {str(e)}"
        )


@router.get("/config/schemas/{warehouse_type}")
async def get_warehouse_config_schema(
    warehouse_type: WarehouseType,
    current_user: User = Depends(require_permissions("data_warehouse:config:read")),
) -> StandardResponse[dict[str, Any]]:
    """Get configuration schema for a specific warehouse type."""
    try:
        schema = ConnectorFactory.get_config_schema(warehouse_type)

        return StandardResponse(
            success=True,
            data={"warehouse_type": warehouse_type.value, "schema": schema},
            message=f"Schema retrieved for {warehouse_type.value}",
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get configuration schema: {str(e)}"
        )


@router.post("/config/validate")
async def validate_warehouse_config(
    warehouse_type: WarehouseType,
    config: dict[str, Any] = Body(...),
    current_user: User = Depends(require_permissions("data_warehouse:config:validate")),
) -> StandardResponse[dict[str, Any]]:
    """Validate warehouse configuration parameters."""
    try:
        # Validate the configuration
        validated_config = ConnectorFactory.validate_config(warehouse_type, config)

        return StandardResponse(
            success=True,
            data={
                "warehouse_type": warehouse_type.value,
                "valid": True,
                "validated_config": validated_config.model_dump(
                    exclude={"password", "private_key", "private_key_passphrase"}
                ),
            },
            message=f"Configuration is valid for {warehouse_type.value}",
        )

    except ValueError as e:
        return StandardResponse(
            success=False,
            data={
                "warehouse_type": warehouse_type.value,
                "valid": False,
                "errors": [str(e)],
            },
            message=f"Configuration validation failed for {warehouse_type.value}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to validate configuration: {str(e)}"
        )
