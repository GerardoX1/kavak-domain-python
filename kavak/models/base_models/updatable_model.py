from time import time
from typing import Any

from pydantic import BaseModel, ConfigDict, PositiveInt, model_validator


class UpdatableModel(BaseModel):
    updated_at: PositiveInt

    model_config = ConfigDict(use_enum_values=True, populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def _set_updated_at_as_created_at(cls, data: Any) -> Any:
        data["updated_at"] = data.get(
            "updated_at", data.get("created_at", round(time() * 1000))
        )
        return data

    def update(self, data: dict):
        updated_model = self.model_validate(
            {
                **self.model_dump(),
                **data,
                "updated_at": round(time() * 1000),
            }
        )
        for k in updated_model.model_fields_set:
            setattr(self, k, getattr(updated_model, k))
        return self
