from kavak.models.v1.chatbot_conversations import ChatbotConversationModel
from kavak.services.base_services.base_service import BaseService


class ChatbotConversationService(BaseService[ChatbotConversationModel]):
    __entity_model__ = ChatbotConversationModel

    def __init__(self, repository: object, verbose: bool = False, *args, **kwargs):
        self.__repository__ = repository
        self.__verbose__ = verbose
        super().__init__(*args, **kwargs)
