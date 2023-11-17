from pydantic import BaseModel

class Item(BaseModel):
    id: int
    order_id: int
    description: str
    price: float
    
    @classmethod
    def _convert_to_real_type_(cls, data):
        data_type = data.get("type")

        if data_type is None:
            raise ValueError("Missing 'type' in Item")

        sub = cls._subtypes_.get(data_type)

        if sub is None:
            raise TypeError(f"Unsupport sub-type: {data_type}")

        return sub(**data)
    
    @classmethod
    def parse_obj(cls, obj):
        return cls._convert_to_real_type_(obj)