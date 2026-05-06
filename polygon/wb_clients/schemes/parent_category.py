from pydantic import BaseModel, Field


class ParentCategory(BaseModel):
    id: int
    name: str
    is_visible: bool = Field(..., validation_alias="isVisible")
