from typing import List
import os
import azure.functions as func
import logging

from SharedCode.CosmosItem import CosmosItem
from SharedCode.Item import Item

from azure.cosmos import CosmosClient

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.propagate import extract

root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

configure_azure_monitor()

url = os.environ["COSMOS_URI"]
key = os.environ["COSMOS_KEY"]
client = CosmosClient(url, key)

database = client.get_database_client(os.environ["COSMOS_DATABASE_NAME"])
container = database.get_container_client(os.environ["COSMOS_CONTAINER_NAME"])

def main(ingestion: List[func.EventHubEvent], context) -> None:
    carrier = {
        "traceparent": context.trace_context.Traceparent,
        "tracestate": context.trace_context.Tracestate,
    }
    tracer = trace.get_tracer(__name__)
    
    with tracer.start_as_current_span("transform_items", context = extract(carrier)):
        cosmos_items: List[CosmosItem] = []
        for raw_item in ingestion:
            item = Item.model_validate_json(raw_item.get_body().decode('utf-8'))
            if item.id % 7 != 0:
                cosmos_items.append(CosmosItem(id=str(item.id), order_id=str(item.order_id), description=item.description, price=item.price))

        logging.info(f"Transform: Transformed {len(cosmos_items)} items.")
        for cosmos_item in cosmos_items:
            container.upsert_item(body=cosmos_item.model_dump())
