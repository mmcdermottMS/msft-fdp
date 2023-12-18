import logging
import os
from typing import List

import azure.functions as func
from azure.cosmos import CosmosClient

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.propagate import extract

from SharedCode.CosmosItem import CosmosItem
from SharedCode.Item import Item

configure_azure_monitor()

client = CosmosClient(os.environ["COSMOS_URI"], os.environ["COSMOS_KEY"])
database = client.get_database_client(os.environ["COSMOS_DATABASE_NAME"])
container = database.get_container_client(os.environ["COSMOS_CONTAINER_NAME"])

def main(transformation: List[func.EventHubEvent], context) -> None:
    carrier = {
        "traceparent": context.trace_context.Traceparent,
        "tracestate": context.trace_context.Tracestate,
    }
    
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("transform_items", context = extract(carrier)):
        cosmos_items: List[CosmosItem] = []
        for raw_item in transformation:
            item = Item.model_validate_json(raw_item.get_body().decode('utf-8'))
            if item.id % 7 != 0:
                cosmos_items.append(CosmosItem(id=str(item.id), order_id=str(item.order_id), description=item.description, price=item.price))

        logging.info(f"FDP - Transform: Transformed {len(cosmos_items)} items.")
        for cosmos_item in cosmos_items:
            container.upsert_item(body=cosmos_item.model_dump())
