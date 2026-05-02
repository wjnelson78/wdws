from typing import Any, Literal

from pydantic import BaseModel, Field


class ConfigValueIn(BaseModel):
    value: Any
    source: Literal["principal", "adaptation", "default"] = "principal"


class ConfigValueOut(BaseModel):
    parameter_path: str
    value: Any
    source: str


class ConfigBundleOut(BaseModel):
    user_identifier: str
    parameters: dict[str, Any] = Field(default_factory=dict)
