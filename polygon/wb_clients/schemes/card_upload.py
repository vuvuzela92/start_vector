from typing import Optional, Union

from pydantic import BaseModel, Field

from polygon.wb_clients.schemes.card import CardWholesale


class CardDimensionsCreate(BaseModel):
    """Габариты для создания карточки (формат WB API)."""

    width: int = Field(..., description="Ширина, см")
    height: int = Field(..., description="Высота, см")
    length: int = Field(..., description="Длина, см")
    weight_brutto: float = Field(..., serialization_alias="weightBrutto", description="Вес брутто, кг")


class CardCharcCreate(BaseModel):
    """Характеристика для создания карточки."""

    id: int = Field(..., description="ID характеристики")
    value: Union[int, float, list[str]] = Field(..., description="Значение", examples=["Красный"])


class CardSizeCreate(BaseModel):
    """Размер для создания карточки."""

    tech_size: str = Field(..., serialization_alias="techSize", description="Технический размер")
    wb_size: str = Field(..., serialization_alias="wbSize", description="Размер по WB")


class CardVariantRequest(BaseModel):
    """Вариант карточки (без vendor_code) для запроса на создание."""

    brand: Optional[str] = Field(None, description="Бренд")
    title: Optional[str] = Field(None, description="Название")
    description: Optional[str] = Field(None, description="Описание")
    wholesale: Optional[CardWholesale] = Field(None, description="Оптовые настройки")
    dimensions: CardDimensionsCreate = Field(..., description="Габариты")
    characteristics: Optional[list[CardCharcCreate]] = Field(None, description="Характеристики")
    sizes: Optional[list[CardSizeCreate]] = Field(None, description="Размеры")


class CardVariant(CardVariantRequest):
    """Вариант карточки с vendor_code (для WB API)."""

    vendor_code: str = Field(..., serialization_alias="vendorCode", description="Артикул продавца")


class CardCreate(BaseModel):
    """Модель для создания карточки в WB API."""

    subject_id: int = Field(..., serialization_alias="subjectID", description="ID предмета")
    variants: list[CardVariant] = Field(..., description="Список вариантов")


class WBCardCreateRequest(BaseModel):
    """Запрос на создание карточки (внутренний формат CRM)."""

    subject_id: int = Field(..., description="ID предмета")
    local_vendor_code: str = Field(..., description="Локальный артикул продавца")
    variants: list[CardVariantRequest] = Field(..., description="Варианты карточки")

class UploadWBCardsRequest(BaseModel):
    """Запрос на пакетное создание карточек."""

    account: str = Field(..., description="Аккаунт")
    data: list[WBCardCreateRequest] = Field(..., description="Список карточек для создания")