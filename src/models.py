from typing import Dict
from pydantic import BaseModel, RootModel


class ColumnMetadataItem(BaseModel):
    metric: bool
    dimension: bool


class ColumnMetadata(RootModel):
    root: Dict[str, ColumnMetadataItem]


class RequestData(BaseModel):
    column_metadata: ColumnMetadata
