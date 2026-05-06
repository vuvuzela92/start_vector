from pydantic import BaseModel, Field


class Characteristic(BaseModel):
    id: int = Field(..., validation_alias="charcID")
    name: str
    subject_name: str = Field(..., validation_alias="subjectName")
    subject_id: int = Field(..., validation_alias="subjectID")
    required: bool
    unit_name: str = Field(..., validation_alias="unitName")
    max_count: int = Field(..., validation_alias="maxCount")
    popular: bool
    charc_type: int = Field(..., validation_alias="charcType")


class Color(BaseModel):
    name: str
    parent_name: str = Field(..., validation_alias="parentName")


class Country(BaseModel):
    id: int
    name: str
    full_name: str = Field(..., validation_alias="fullName")


class Brand(BaseModel):
    id: int
    logo_url: str = Field(..., validation_alias="logoUrl")
    name: str
