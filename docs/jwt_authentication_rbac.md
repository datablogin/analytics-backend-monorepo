# JWT Authentication & Role-Based Access Control (RBAC)

## Overview

This document describes the JWT authentication and RBAC system implemented for the Analytics Backend API. The system provides secure user authentication, authorization, and role-based access control.

## Architecture

### Components

1. **Authentication Service** (`libs/analytics_core/auth.py`)
   - JWT token creation and validation
   - Password hashing with bcrypt
   - User authentication logic
   - Permission and role checking

2. **Database Models** (`libs/analytics_core/models.py`)
   - User: User accounts with credentials
   - Role: Permission groups with specific capabilities
   - UserRole: Many-to-many relationship between users and roles

3. **API Routes**
   - `/auth/*`: Authentication endpoints (login, register, etc.)
   - `/admin/*`: Administrative endpoints for role management

## Features

### Authentication
- **User Registration**: Create new user accounts with email validation
- **User Login**: JWT token-based authentication
- **Password Management**: Secure password hashing and change functionality
- **Token Validation**: JWT token verification with expiration handling

### Authorization
- **Role-Based Access Control**: Users assigned to roles with specific permissions
- **Permission-Based Access**: Fine-grained access control using permission strings
- **Protected Endpoints**: Decorator-based endpoint protection

### Security Features
- **Password Hashing**: bcrypt hashing with salts
- **JWT Tokens**: Secure token generation with configurable expiration
- **CORS Protection**: Configurable cross-origin request handling
- **Input Validation**: Pydantic models for request/response validation

## API Endpoints

### Authentication Endpoints

#### POST `/auth/register`
Register a new user account.

**Request Body:**
```json
{
  "username": "string",
  "email": "user@example.com",
  "password": "string",
  "full_name": "string"
}
```

**Response:**
```json
{
  "id": 1,
  "username": "string",
  "email": "user@example.com",
  "full_name": "string",
  "is_active": true
}
```

#### POST `/auth/token`
Login and receive access token.

**Request Body (form data):**
```
username=your_username
password=your_password
```

**Response:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "token_type": "bearer",
  "expires_in": 1800
}
```

#### GET `/auth/me`
Get current user information (requires authentication).

**Headers:**
```
Authorization: Bearer <token>
```

**Response:**
```json
{
  "id": 1,
  "username": "string",
  "email": "user@example.com",
  "full_name": "string",
  "is_active": true
}
```

#### POST `/auth/change-password`
Change user password (requires authentication).

**Request Body:**
```json
{
  "current_password": "string",
  "new_password": "string"
}
```

### Administrative Endpoints

#### POST `/admin/roles`
Create a new role (requires `admin:roles:create` permission).

**Request Body:**
```json
{
  "name": "string",
  "description": "string",
  "permissions": ["permission1", "permission2"]
}
```

#### GET `/admin/roles`
List all active roles (requires `admin:roles:read` permission).

#### PUT `/admin/roles/{role_id}`
Update a role (requires `admin:roles:update` permission).

#### DELETE `/admin/roles/{role_id}`
Delete a role (requires `admin:roles:delete` permission).

#### POST `/admin/users/{user_id}/roles`
Assign role to user (requires `admin:users:assign_roles` permission).

#### GET `/admin/users`
List users with their roles (requires `admin:users:read` permission).

## Permission System

### Permission Format
Permissions follow the format: `resource:action` or `namespace:resource:action`

Examples:
- `read:data` - Read data resources
- `write:data` - Write data resources
- `admin:roles:create` - Create roles in admin namespace
- `admin:users:assign_roles` - Assign roles to users

### Built-in Permissions
- `admin:roles:create` - Create new roles
- `admin:roles:read` - View roles
- `admin:roles:update` - Modify existing roles
- `admin:roles:delete` - Delete roles
- `admin:users:read` - View users and their roles
- `admin:users:assign_roles` - Assign roles to users
- `admin:users:remove_roles` - Remove roles from users

## Usage Examples

### 1. User Registration and Login

```python
# Register a new user
response = requests.post("http://localhost:8000/auth/register", json={
    "username": "john_doe",
    "email": "john@example.com",
    "password": "secure_password123",
    "full_name": "John Doe"
})

# Login to get token
response = requests.post("http://localhost:8000/auth/token", data={
    "username": "john_doe",
    "password": "secure_password123"
})
token = response.json()["access_token"]
```

### 2. Accessing Protected Endpoints

```python
headers = {"Authorization": f"Bearer {token}"}

# Access user info
response = requests.get("http://localhost:8000/auth/me", headers=headers)

# Access protected endpoint
response = requests.get("http://localhost:8000/protected", headers=headers)
```

### 3. Role Management (Admin)

```python
# Create a new role
response = requests.post("http://localhost:8000/admin/roles", 
    headers=headers,
    json={
        "name": "data_analyst",
        "description": "Data analysis role",
        "permissions": ["read:data", "analyze:data"]
    }
)

# Assign role to user
response = requests.post(f"http://localhost:8000/admin/users/{user_id}/roles",
    headers=headers,
    json={"user_id": user_id, "role_id": role_id}
)
```

### 4. Using Permission Decorators

```python
from libs.analytics_core.auth import require_permissions, require_roles

@app.get("/analytics/reports")
async def get_reports(user=Depends(require_permissions("read:reports"))):
    return {"reports": []}

@app.post("/analytics/models")
async def create_model(user=Depends(require_roles("data_scientist"))):
    return {"message": "Model created"}
```

## Configuration

### Environment Variables

Create a `.env` file or set environment variables:

```bash
# JWT Configuration
JWT_SECRET_KEY=your-super-secret-key-change-in-production
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=30

# Database Configuration
DATABASE_URL=postgresql://user:password@localhost/analytics_db
```

### Security Configuration

```python
# In production, update these settings:

# 1. Change the secret key
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-this-in-production")

# 2. Configure CORS appropriately
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],  # Specific domains only
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# 3. Use HTTPS in production
# 4. Set up proper database connection pooling
# 5. Configure rate limiting
```

## Testing

### Running Tests

```bash
# Install test dependencies
make dev-install

# Run authentication tests
pytest tests/test_jwt_authentication.py -v

# Run all tests
make test
```

### Test Coverage

The test suite covers:
- Password hashing and verification
- JWT token creation and validation
- User registration and authentication
- Role-based access control
- Permission checking
- API endpoint security
- Error handling and edge cases

## Security Considerations

### Best Practices Implemented

1. **Password Security**
   - bcrypt hashing with automatic salting
   - No plaintext password storage
   - Password change validation

2. **JWT Security**
   - Configurable token expiration
   - Secure token signing
   - Token validation on each request

3. **Access Control**
   - Role-based permissions
   - Fine-grained permission system
   - Endpoint-level protection

4. **Input Validation**
   - Pydantic model validation
   - Email format validation
   - SQL injection prevention through ORM

### Production Security Checklist

- [ ] Change default JWT secret key
- [ ] Configure appropriate CORS settings
- [ ] Enable HTTPS/TLS
- [ ] Set up rate limiting
- [ ] Configure secure session management
- [ ] Enable audit logging
- [ ] Set up monitoring and alerting
- [ ] Regular security updates
- [ ] Database connection encryption
- [ ] Input sanitization

## Performance Considerations

### Optimization Strategies

1. **Token Caching**: Consider Redis for token blacklisting
2. **Permission Caching**: Cache user permissions to reduce DB queries
3. **Connection Pooling**: Use proper database connection pooling
4. **Async Operations**: All database operations are async
5. **Index Optimization**: Ensure proper database indexes on user/role lookups

### Monitoring

Monitor these metrics:
- Authentication success/failure rates
- Token validation performance
- Role assignment frequency
- Permission check latency
- Database query performance

## Troubleshooting

### Common Issues

1. **Token Expired Error**
   - Check token expiration settings
   - Implement token refresh mechanism if needed

2. **Permission Denied**
   - Verify user roles and permissions
   - Check permission string format

3. **Authentication Failures**
   - Verify password hashing compatibility
   - Check user account status (is_active)

4. **Database Connection Issues**
   - Verify database configuration
   - Check connection pooling settings

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

This will provide detailed information about authentication flows and permission checks.