from enum import Enum
from time import time
from typing import List, Literal

from kavak.models.base_models.base_model import BaseModel
from pydantic import Field, PositiveInt


class RoleTypes(str, Enum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"


class MessagesQAModel(BaseModel):
    role: RoleTypes
    content: str


class ChatbotConversationModel(BaseModel):
    __collection_name__: str = "chatbot-conversations"
    version: Literal["1.0.0"] = "1.0.0"
    updated_at: PositiveInt = Field(default_factory=lambda: round(time() * 1000))
    messages: List[MessagesQAModel] = []
