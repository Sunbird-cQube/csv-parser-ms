from typing import Dict
from pydantic import BaseModel, RootModel


class ColumnMetadataItem(BaseModel):
    updated_col_name: str
    metric: bool
    dimension: bool


class ColumnMetadata(RootModel):
    root: Dict[str, ColumnMetadataItem]


class RequestData(BaseModel):
    column_metadata: ColumnMetadata
