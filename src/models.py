from typing import Dict
from pydantic import BaseModel, RootModel, field_validator, constr
from typing import List

class ColumnMetadataItem(BaseModel):
    updated_col_name: str
    type: str
    metric: bool
    dimension: bool


class ColumnMetadata(RootModel):
    root: Dict[str, ColumnMetadataItem]

class DimensionFieldDataValueModel(BaseModel):
    updated_col_name: str
    type: str
    metric: bool

class DimensionFieldDataModel(BaseModel):
    key: str
    values: DimensionFieldDataValueModel

class DimensionFieldModel(BaseModel):
    value: str
    isPrimary: bool
    isIndex: bool
    data: DimensionFieldDataModel

class DimensionModel(BaseModel):
    name: str
    fields: List[DimensionFieldModel]

class RequestData(BaseModel):
    program_name: constr(
        strip_whitespace=True,
        min_length=1,
        max_length=255,
        strict=True,
    )
    program_desc: str
    column_metadata: ColumnMetadata
    dimensions: List[DimensionModel]

    @field_validator("program_name")
    @classmethod
    def validate_program_name(cls, value):
        # Check if the program_name is path-safe (no special characters except "-", "_")
        # You can add more checks as needed
        if not all(char.isalnum() or char in ("-", "_") for char in value):
            raise ValueError("program_name must be path-safe (alphanumeric, '-', '_')")
        return value
