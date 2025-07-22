"""Configuration base class for ML models."""

from pydantic import BaseModel


class BaseConfig(BaseModel):
    """Base configuration class for ML components."""

    class Config:
        """Pydantic configuration."""

        extra = "forbid"
        validate_assignment = True
        use_enum_values = True
        frozen = False
