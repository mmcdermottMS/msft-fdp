from pydantic import BaseModel

class Item(BaseModel):
    id: int
    order_id: int
    description: str
    price: float