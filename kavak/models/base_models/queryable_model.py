from typing import Any, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.fields import FieldInfo


class QueryableModel(BaseModel):
    limit: int = Field(default=50, gt=0, lt=51, alias="page_size")
    page: int = Field(default=1, gt=0, alias="page_number")
    sort: Optional[str | List[Tuple[str, int]]] = None

    model_config = ConfigDict(use_enum_values=True, populate_by_name=True)

    @field_validator("sort", mode="after")
    @classmethod
    def _convert_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            key, direction = v.split(":")
            v = [(key, 1 if "asc" in direction else -1)]
        return v

    @staticmethod
    def _is_filter_field(field: FieldInfo, filter_type: str) -> bool:
        return (
            field.json_schema_extra is not None
            and field.json_schema_extra.get(filter_type) is True
        )

    def filters(
        self,
        exclude_unset: bool = True,
        exclude_defaults: bool = True,
        exclude_none: bool = True,
    ) -> List[tuple]:
        return [
            (
                self.model_fields[k].alias or k,
                self.model_fields[k].json_schema_extra.get("condition"),
                v,
            )
            for k, v in self.model_dump(
                include={
                    attribute
                    for attribute, field in self.model_fields.items()
                    if self._is_filter_field(field, "filter")
                },
                exclude_unset=exclude_unset,
                exclude_defaults=exclude_defaults,
                exclude_none=exclude_none,
            ).items()
        ]
