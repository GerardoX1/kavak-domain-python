from abc import ABC, abstractmethod
from typing import Optional


class BaseServiceABC(ABC):
    """Abstract base class for defining a generic service interface."""

    @abstractmethod
    def get(self, document_id: str) -> Optional[object]:
        """Retrieve a document based on its unique identifier.

        Args:
            document_id (str): The unique identifier of the document.

        Returns:
            Optional[object]: The retrieved document, or None if not
                found.
        """
        ...

    @abstractmethod
    def create(self, document_data: object) -> Optional[object]:
        """Create a new document in the data storage system.

        Args:
            document_data (object): The data representing the document
                to be created.

        Returns:
            Optional[object]: The created document, or None if creation
                fails.
        """
        ...

    @abstractmethod
    def update(self, document_data: object) -> Optional[object]:
        """Update an existing document with new data.

        Args:
            document_data (object): The data representing the document
                to be updated.

        Returns:
            Optional[object]: The updated document, or None if the
                update fails.
        """
        ...
