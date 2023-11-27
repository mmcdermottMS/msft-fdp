from typing import List
import os
import azure.functions as func
import logging

from azure.cosmos import CosmosClient

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
configure_azure_monitor()

from SharedCode.CosmosItem import CosmosItem
from SharedCode.Item import Item

#trace.set_tracer_provider(TracerProvider())
#tracer = trace.get_tracer(__name__)
#exporter = AzureMonitorTraceExporter(connection_string=os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'])
#trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(exporter))

url = os.environ["COSMOS_URI"]
key = os.environ["COSMOS_KEY"]
client = CosmosClient(url, key)

database = client.get_database_client(os.environ["COSMOS_DATABASE_NAME"])
container = database.get_container_client(os.environ["COSMOS_CONTAINER_NAME"])

def main(ingestion: List[func.EventHubEvent]) -> None:
    #with tracer.start_as_current_span("transform_items"):       
    cosmos_items: List[CosmosItem] = []
    for raw_item in ingestion:
        logging.info(f"DistTrace: {raw_item.metadata['PropertiesArray']}")
        item = Item.model_validate_json(raw_item.get_body().decode('utf-8'))
        if item.id % 7 != 0:
            cosmos_items.append(CosmosItem(id=str(item.id), order_id=str(item.order_id), description=item.description, price=item.price))

    logging.info(f"Transform: Transformed {len(cosmos_items)} items.")
    for cosmos_item in cosmos_items:
        container.upsert_item(body=cosmos_item.model_dump())
