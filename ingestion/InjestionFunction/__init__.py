from typing import List

import json
import azure.functions as func
import logging

from SharedCode.Item import Item

def main(ingestion: List[func.EventHubEvent]) -> None:
    logging.info(f"Ingested {len(ingestion) - 1} events.")
    
    items: List[Item] = []
    for raw_item in ingestion:
        item = json.loads(raw_item.get_body().decode('utf-8'), object_hook=lambda d: Item(**d))
        items.append(item)

    for item in items:
        logging.info('Python EventHub trigger processed an event: %s', item.description)
        