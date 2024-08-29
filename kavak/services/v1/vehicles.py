from typing import Optional

from kavak.models.v1.vehicles import VehiclesModel
from kavak.services.base_services.base_service import BaseService


class VehiclesService(BaseService[VehiclesModel]):
    __entity_model__ = VehiclesModel

    def __init__(self, repository: object, verbose: bool = False, *args, **kwargs):
        self.__repository__ = repository
        self.__verbose__ = verbose
        super().__init__(*args, **kwargs)

    def get_by_stoke(self, stock_id: str) -> Optional[VehiclesModel]:
        query_result = self._query_one(and_conditions=[("stock_id", "==", stock_id)])
        if not query_result:
            return None
        return self.__entity_model__(**query_result)
