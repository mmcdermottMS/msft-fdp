from typing import List

from SharedCode.Item import Item

class Order:
    def __init__(self, id: str, items: List[Item]):
        self.id = id
        self.items = items