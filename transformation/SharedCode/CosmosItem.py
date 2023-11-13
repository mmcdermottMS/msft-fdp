from pydantic import BaseModel

class CosmosItem(BaseModel):
    id: str
    order_id: str
    description: str
    price: float