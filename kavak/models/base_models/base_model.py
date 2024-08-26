from time import time
from typing import TypeVar
from uuid import NAMESPACE_OID, uuid4, uuid5

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict, Field, PositiveInt

BaseModelT = TypeVar("BaseModelT", bound="BaseModel")


class BaseModel(PydanticBaseModel):
    __version__: str = "1.0.0"
    __collection_name__: str

    id: str = Field(default_factory=lambda: str(uuid4()), alias="_id")
    created_at: PositiveInt = Field(default_factory=lambda: round(time() * 1000))

    model_config = ConfigDict(use_enum_values=True, populate_by_name=True)


def uuid_by_params(*args):
    value = "#".join(map(str, args))
    return str(uuid5(namespace=NAMESPACE_OID, name=value))
