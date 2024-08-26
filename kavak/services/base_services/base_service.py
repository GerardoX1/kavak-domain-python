from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar

from kavak.models.base_models.base_model import BaseModelT
from kavak.services.base_services.abstract_service import BaseServiceABC

MongoRepository = TypeVar("MongoRepository")
CommandCursor = TypeVar("CommandCursor")
Query = TypeVar("Query")


class BaseService(BaseServiceABC, Generic[BaseModelT]):
    """A generic base service class for interacting with a data storage
    system.

    Attributes:
        __repository__: The repository for data storage.
        __entity_model__: The type of the entity model used in the
            service.
        __entity_model_collection__: The name of the collection for the
            entity model.
        __verbose__: A flag indicating verbosity in service operations
        (default is False).
    """

    __repository__: MongoRepository
    __entity_model__: Type[BaseModelT]
    __entity_model_collection__: str
    __verbose__: bool = False

    def __init__(self, *args, **kwargs):
        self._validate_entity_model()
        self._set_entity_model_collection()

    def _validate_entity_model(self) -> None:
        """Validates that the __entity_model__ class is defined.

        Raises:
            TypeError: If the __entity_model__ class is not defined.
        """
        if not getattr(self, "__entity_model__", None):
            raise TypeError(
                f"__entity_model__ class must be defined at "
                f"{self.__class__.__name__} service"
            )

    def _set_entity_model_collection(self) -> None:
        """Sets the entity model collection name based on the
        __entity_model__ class."""
        self.__entity_model_collection__ = self.__entity_model__.__collection_name__

    def _instantiate_entity_model(self, data: Dict[str, Any]) -> BaseModelT:
        """Instantiate an entity model using the provided data.

        Parameters:
            data (Dict[str, Any]): The data used to create the entity
                model.

        Returns:
            BaseModelT: An instance of the entity model.
        """
        return self.__entity_model__.model_validate(data)

    def get(self, document_id: str) -> Optional[BaseModelT]:
        """Retrieve a document based on its unique identifier.

        Parameters:
            document_id (str): The unique identifier of the document.

        Returns:
            Optional[BaseModelT]: An instance of the BaseModelT
                representing the retrieved document, or None if the
                document is not found.
        """
        document_data: Dict[str, Any] | None = self.__repository__.get(
            self.__entity_model_collection__, document_id
        )
        return self._instantiate_entity_model(document_data) if document_data else None

    def create(self, document_data: Dict[str, Any]) -> BaseModelT:
        """Create a new document in the data storage system.

        Parameters:
            document_data (Dict[str, Any]): The data representing the
                document to be created.

        Returns:
            BaseModelT: An instance of the BaseModelT representing the
                newly created document.
        """
        model_instance = self._instantiate_entity_model(document_data)
        self.__repository__.create(
            self.__entity_model_collection__,
            model_instance.model_dump(by_alias=True),
        )
        return model_instance

    def update(
        self,
        document_data: Dict[str, Any],
    ) -> Optional[BaseModelT]:
        """Update an existing document with new data.

        Parameters:
            document_data (Dict[str, Any]): The data representing the
                document to be updated.

        Returns:
            Optional[BaseModelT]: An instance of the BaseModelT
                representing the updated document, or None if the update
                fails.
        """
        model_instance = self._instantiate_entity_model(document_data)
        result_count = self.__repository__.update(
            self.__entity_model_collection__,
            model_instance.id,
            model_instance.model_dump(by_alias=True),
        )
        return model_instance if result_count != 0 else None

    def set(
        self,
        document_data: Dict[str, Any],
    ) -> Optional[BaseModelT]:
        """Set or replace an existing document with new data.

        Parameters:
            document_data (Dict[str, Any]): The data representing the
                document to be set or replaced.

        Returns:
            Optional[BaseModelT]: An instance of the BaseModelT
                representing the set or replaced document, or None if
                the operation fails.
        """
        model_instance = self._instantiate_entity_model(document_data)
        result_count = self.__repository__.set(
            self.__entity_model_collection__,
            model_instance.id,
            model_instance.model_dump(by_alias=True),
        )
        return model_instance if result_count != 0 else None

    def __base_query(
        self,
        and_conditions: Optional[List[tuple]] = None,
        or_conditions: Optional[List[tuple]] = None,
    ) -> Query:
        """Construct a base query with optional "AND" and "OR"
        conditions.

        Parameters:
            and_conditions (Optional[List[tuple]]): List of tuples
                representing AND conditions.
            or_conditions (Optional[List[tuple]]): List of tuples
                representing OR conditions.

        Returns:
            Query: The Query.
        """
        query: Query = self.__repository__.query(self.__entity_model_collection__)
        if and_conditions:
            query.and_search(and_conditions)
        if or_conditions:
            query.or_search(or_conditions)
        return query

    def _query(
        self,
        and_conditions: Optional[List[tuple]] = None,
        or_conditions: Optional[List[tuple]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
        limit: int = None,
    ) -> List[dict]:
        """Execute a query and return a list of matching documents.

        Parameters:
            and_conditions (Optional[List[tuple]]): List of tuples
                representing AND conditions.
            or_conditions (Optional[List[tuple]]): List of tuples
                representing OR conditions.
            sort (Optional[List[Tuple[str, int]]]): List of tuples
                representing sorting criteria.
            projection (Optional[List[str]]): List of fields to be
                included in the result.
            limit (int): Maximum number of documents to retrieve.

        Returns:
            List[dict]: List of matching documents.
        """
        query = self.__base_query(and_conditions, or_conditions)
        kwargs = {"sort": sort, "projection": projection}
        if limit:
            kwargs.update({"limit": limit})
        return list(query.get_all(**kwargs))

    def _query_with_count(
        self,
        and_conditions: Optional[List[tuple]] = None,
        or_conditions: Optional[List[tuple]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
        limit: int = None,
    ) -> Tuple[int, List[dict]]:
        """Execute a query, return a list of matching documents, and the
        count of total matching documents.

        Parameters:
            and_conditions (Optional[List[tuple]]): List of tuples
                representing AND conditions.
            or_conditions (Optional[List[tuple]]): List of tuples
                representing OR conditions.
            sort (Optional[List[Tuple[str, int]]]): List of tuples
                representing sorting criteria.
            projection (Optional[List[str]]): List of fields to be
                included in the result.
            limit (int): Maximum number of documents to retrieve.

        Returns:
            Tuple[int, List[dict]]: A tuple containing the count of
                total matching documents and the list of documents.
        """
        query = self.__base_query(and_conditions, or_conditions)
        kwargs = {"sort": sort, "projection": projection}
        if limit:
            kwargs.update({"limit": limit})
        return query.count(), list(query.get_all(**kwargs))

    def _query_one(
        self,
        and_conditions: Optional[List[tuple]] = None,
        or_conditions: Optional[List[tuple]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
    ) -> Optional[dict]:
        """Execute a query and return the first matching document.

        Parameters:
            and_conditions (Optional[List[tuple]]): List of tuples
                representing AND conditions.
            or_conditions (Optional[List[tuple]]): List of tuples
                representing OR conditions.
            sort (Optional[List[Tuple[str, int]]]): List of tuples
                representing sorting criteria.
            projection (Optional[List[str]]): List of fields to be
                included in the result.

        Returns:
            Optional[dict]: The first matching document, or None if no
                documents match the criteria.
        """
        query = self.__base_query(and_conditions, or_conditions)
        return query.get_one_or_none(sort=sort, projection=projection)

    def _query_paginated(
        self,
        page: int = 1,
        limit: int = 50,
        and_conditions: Optional[List[tuple]] = None,
        or_conditions: Optional[List[tuple]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
    ) -> Tuple[int, List[dict]]:
        """Execute a paginated query and return a tuple containing the
        count of total matching documents and a list of documents for
        the specified page.

        Parameters:
            page (int): The page number for paginated results
                (default is 1).
            limit (int): The maximum number of documents per page
                (default is 50).
            and_conditions (Optional[List[tuple]]): List of tuples
                representing AND conditions.
            or_conditions (Optional[List[tuple]]): List of tuples
                representing OR conditions.
            sort (Optional[List[Tuple[str, int]]]): List of tuples
                representing sorting criteria.
            projection (Optional[List[str]]): List of fields to be
                included in the result.

        Returns:
            Tuple[int, List[dict]]: A tuple containing the count of
                total matching documents and the list of documents for
                the specified page.
        """
        query = self.__base_query(and_conditions, or_conditions)
        return query.count(), list(
            query.paginate(page, limit, sort=sort, projection=projection)
        )
