from src.wb_clients.base.client import HTTPMethod
from src.wb_clients.schemes.parent_category import ParentCategory
from src.wb_clients.wb_content import ContentWBAPI
from src.logging import get_logger


logger = get_logger(__name__)


class ParentCategoriesWBAPI(ContentWBAPI):
    """API-клиент WB для родительских категорий."""
    GET_PARENTS_ENDPOINT = "/content/v2/object/parent/all"

    async def get_all_categories(self):
        """Метод возвращает все родительские категории."""
        response = await self._make_request(
            endpoint=self.GET_PARENTS_ENDPOINT,
            method=HTTPMethod.GET,
        )

        return [ParentCategory.model_validate(pc) for pc in response.get("data", [])]
