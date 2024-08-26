from typing import (Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar,
                    Union)

from partners.models.base_models.base_model import BaseModelT
from partners.services.base_services.abstract_service import BaseServiceABC

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

    def _update_many(self, and_conditions: List[tuple], data: Dict[str, Any]) -> int:
        """Update multiple documents based on specified conditions.

        Parameters:
            and_conditions (List[tuple]): A list of tuples
                representing AND conditions for the update
                operation. Each tuple should contain a field
                name, a comparison operator, and a value.
            data (Dict[str, Any]): The new data to be applied to
                the matching documents.

        Returns:
            int: The number of documents successfully updated.
        """
        result_count = self.__repository__.update_many(
            self.__entity_model_collection__, and_conditions, data
        )
        return result_count

    def _push_array(self, document_id: str, key: str, value: Any) -> int:
        """Push a value to an array within a specified document.

        Parameters:
            document_id (str): The unique identifier of the document
                containing the array.
            key (str): The key representing the array within the
                document.
            value (Any): The value to be pushed to the array.

        Returns:
            int: The number of documents successfully updated.
        """
        result_count = self.__repository__.push_array(
            self.__entity_model_collection__,
            document_id=document_id,
            key=key,
            value=value,
        )
        return result_count

    def _push_array_many(
        self,
        document_id: str,
        key: str,
        values: List[Any],
        document_data: Optional[dict] = None,
        avoid_duplication: bool = False,
    ) -> int:
        """Push multiple values to an array within a specified document.

        Parameters:
            document_id (str): The unique identifier of the document
                containing the array.
            key (str): The key representing the array within the
                document.
            values (List[Any]): The list of values to be pushed to the
                array.
            document_data (Optional[dict]): Additional data to update in
                the document alongside the array push.
            avoid_duplication (bool): Flag to avoid duplicating values
                in the array.

        Returns:
            int: The number of documents successfully updated.
        """
        result_count = self.__repository__.push_array_many(
            self.__entity_model_collection__,
            document_id=document_id,
            key=key,
            values=values,
            document_data=document_data,
            avoid_duplication=avoid_duplication,
        )
        return result_count

    def _pull_array(
        self,
        document_id: str,
        key: str,
        value: Any,
        document_data: Optional[dict] = None,
    ) -> int:
        """Pull a specific value from an array within a specified
        document.

        Parameters:
            document_id (str): The unique identifier of the document
                containing the array.
            key (str): The key representing the array within the
                document.
            value (Any): The value to be pulled from the array.
            document_data (Optional[dict]): Additional data to update in
                the document alongside the array pull.

        Returns:
            int: The number of documents successfully updated.
        """
        result_count = self.__repository__.pull_array(
            self.__entity_model_collection__,
            document_id=document_id,
            key=key,
            value=value,
            document_data=document_data,
        )
        return result_count

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

    def _query_unlimited(
        self,
        and_conditions: Optional[List[tuple]] = None,
        or_conditions: Optional[List[tuple]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
    ) -> List[dict]:
        """Execute a MongoDB query and return all matching documents
        without a limit.

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
            List[dict]: List of all matching documents.
        """
        query = self.__base_query(and_conditions, or_conditions)
        return list(query.get_all_without_limit(sort=sort, projection=projection))

    def _query_unlimited_with_count(
        self,
        and_conditions: Optional[List[tuple]] = None,
        or_conditions: Optional[List[tuple]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
    ) -> Tuple[int, List[dict]]:
        """Execute a query, return a list of matching documents, and the
        count of total matching documents without a limit.

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
            Tuple[int, List[dict]]: A tuple containing the count of
                total matching documents and the list of documents.
        """
        query = self.__base_query(and_conditions, or_conditions)
        return query.count(), list(
            query.get_all_without_limit(sort=sort, projection=projection)
        )

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

    @staticmethod
    def __base_search(
        index: str,
        text_filters: dict = None,
        autocomplete_filters: dict = None,
        equal_filters: dict = None,
        range_filters: dict = None,
        not_text_filters: dict = None,
        not_autocomplete_filters: dict = None,
        not_equal_filters: dict = None,
        not_null_filters: set = None,
        sort: Optional[dict] = None,
    ) -> dict:
        """Generate the search step for a search operation.

        Parameters:
            index (str): The name of the search index.
            text_filters (dict): Text filters for matching specific
                fields.
            autocomplete_filters (dict): Autocomplete filters for
                partial matching.
            equal_filters (dict): Filters for exact matching.
            range_filters (dict): Filters for range-based matching.
            not_text_filters (dict): Negated text filters.
            not_autocomplete_filters (dict): Negated autocomplete
                filters.
            not_equal_filters (dict): Negated equal filters.
            not_null_filters (set): Filters for non-null values
                matching.
            sort (Optional[dict]): Sorting criteria for the search
                results.

        Returns:
            dict: The search step to be used in a search operation.
        """
        search_step = {
            "index": index,
            "compound": {},
        }

        if text_filters or equal_filters or range_filters or not_null_filters:
            search_step["compound"].update({"filter": []})
        if not_text_filters or not_equal_filters or not_autocomplete_filters:
            search_step["compound"].update({"mustNot": []})

        if text_filters:
            for k, v in text_filters.items():
                search_step["compound"]["filter"].append(
                    {
                        "text": {
                            "query": v,
                            "path": k,
                        }
                    }
                )

        if autocomplete_filters:
            search_step["compound"].update({"should": [], "minimumShouldMatch": 1})
            for k, v in autocomplete_filters.items():
                search_step["compound"]["should"].append(
                    {
                        "autocomplete": {
                            "query": v,
                            "path": k,
                        }
                    }
                )

        if equal_filters:
            for k, v in equal_filters.items():
                search_step["compound"]["filter"].append(
                    {
                        "equals": {
                            "value": v,
                            "path": k,
                        }
                    }
                )

        if range_filters:
            for k, v in range_filters.items():
                search_step["compound"]["filter"].append(
                    {
                        "range": {
                            "path": k,
                            **v,
                        }
                    }
                )

        if not_text_filters:
            for k, v in not_text_filters.items():
                search_step["compound"]["mustNot"].append(
                    {
                        "text": {
                            "query": v,
                            "path": k,
                        }
                    }
                )

        if not_autocomplete_filters:
            for k, v in not_autocomplete_filters.items():
                search_step["compound"]["mustNot"].append(
                    {
                        "autocomplete": {
                            "query": v,
                            "path": k,
                        }
                    }
                )

        if not_equal_filters:
            for k, v in not_equal_filters.items():
                search_step["compound"]["mustNot"].append(
                    {
                        "equals": {
                            "value": v,
                            "path": k,
                        }
                    }
                )

        if not_null_filters:
            for k in not_null_filters:
                search_step["compound"]["filter"].append(
                    {
                        "wildcard": {
                            "query": "*",
                            "path": k,
                            "allowAnalyzedField": True,
                        }
                    }
                )

        if sort:
            search_step.update({"sort": sort})

        return search_step

    def _search_paginated(
        self,
        index: str,
        page: int = 1,
        limit: int = 50,
        text_filters: dict = None,
        autocomplete_filters: dict = None,
        equal_filters: dict = None,
        range_filters: dict = None,
        not_text_filters: dict = None,
        not_autocomplete_filters: dict = None,
        not_equal_filters: dict = None,
        not_null_filters: set = None,
        sort: Optional[dict] = None,
    ) -> Union[Tuple[int, List[dict]], None]:
        """Execute a paginated search operation and return a tuple
        containing the count of total matching documents and a list of
        documents for the specified page.

        Parameters:
            index (str): The name of the search index.
            page (int): The page number for paginated results
                (default is 1).
            limit (int): The maximum number of documents per page
                (default is 50).
            text_filters (dict): Text filters for matching specific
                fields.
            autocomplete_filters (dict): Autocomplete filters for
                partial matching.
            equal_filters (dict): Filters for exact matching.
            range_filters (dict): Filters for range-based matching.
            not_text_filters (dict): Negated text filters.
            not_autocomplete_filters (dict): Negated autocomplete
                filters.
            not_equal_filters (dict): Negated equal filters.
            not_null_filters (set): Filters for non-null values
                matching.
            sort (Optional[dict]): Sorting criteria for the search
                results.

        Returns:
            Union[Tuple[int, List[dict]], None]: A tuple containing the
                count of total matching documents and the list of
                documents for the specified page.
        """
        search_step = self.__base_search(
            index,
            text_filters,
            autocomplete_filters,
            equal_filters,
            range_filters,
            not_text_filters,
            not_autocomplete_filters,
            not_equal_filters,
            not_null_filters,
            sort,
        )
        if search_step.get("compound"):
            collection = self.__repository__.get_collection(
                self.__entity_model_collection__
            )
            pipeline: List[dict] = [
                {
                    "$search": search_step,
                },
                {
                    "$facet": {
                        "results": [
                            {"$skip": limit * (page - 1)},
                            {"$limit": limit},
                        ],
                        "totalCount": [{"$count": "count"}],
                    }
                },
                {"$addFields": {"count": {"$arrayElemAt": ["$totalCount.count", 0]}}},
            ]
            aggregation: CommandCursor = collection.aggregate(pipeline)
            data: dict = aggregation.next()
            count: int = data.get("count", 0)
            documents: List[dict] = data.get("results")
        else:
            query: Query = self.__repository__.query(self.__entity_model_collection__)
            count: int = query.count()
            documents: List[dict] = list(query.paginate(page, limit, sort=sort))

        return count, documents

    def _search_unlimited(
        self,
        index: str,
        text_filters: dict = None,
        autocomplete_filters: dict = None,
        equal_filters: dict = None,
        range_filters: dict = None,
        not_text_filters: dict = None,
        not_autocomplete_filters: dict = None,
        not_equal_filters: dict = None,
        not_null_filters: set = None,
        sort: Optional[dict] = None,
    ) -> List[dict]:
        """Execute an unlimited search operation and return a list of
        all matching documents.

        Parameters:
            index (str): The name of the search index.
            text_filters (dict): Text filters for matching specific
                fields.
            autocomplete_filters (dict): Autocomplete filters for
                partial matching.
            equal_filters (dict): Filters for exact matching.
            range_filters (dict): Filters for range-based matching.
            not_text_filters (dict): Negated text filters.
            not_autocomplete_filters (dict): Negated autocomplete
                filters.
            not_equal_filters (dict): Negated equal filters.
            not_null_filters (set): Filters for non-null values
                matching.
            sort (Optional[dict]): Sorting criteria for the search
                results.

        Returns:
            List[dict]: A list of all matching documents.
        """
        search_step = self.__base_search(
            index,
            text_filters,
            autocomplete_filters,
            equal_filters,
            range_filters,
            not_text_filters,
            not_autocomplete_filters,
            not_equal_filters,
            not_null_filters,
            sort,
        )
        collection = self.__repository__.get_collection(
            self.__entity_model_collection__
        )
        pipeline: List[dict] = [
            {
                "$search": search_step,
            }
        ]
        aggregation: CommandCursor = collection.aggregate(pipeline)

        return list(aggregation)
