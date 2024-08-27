from time import time
from typing import Literal, Optional

from pydantic import Field, PositiveInt

from kavak.models.base_models.base_model import BaseModel


class VehiclesModel(BaseModel):
    __collection_name__: str = "vehicles"
    version: Literal["1.0.0"] = "1.0.0"
    updated_at: PositiveInt = Field(default_factory=lambda: round(time() * 1000))
    stock_id: str
    km: int
    price: int
    make: str
    model: str
    year: int
    version_vehicle: Optional[str] = None
    bluetooth: Optional[bool] = None
    length: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    car_play: Optional[bool] = None
