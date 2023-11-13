import datetime
from typing import List
import json
import os
import azure.functions as func
import logging
from azure.monitor.opentelemetry import configure_azure_monitor
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from opentelemetry import trace
from SharedCode.CosmosItem import CosmosItem

from SharedCode.Item import Item

configure_azure_monitor(connection_string=os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'], service_name="TransformationFunction", instrumentation_key=os.environ['APPINSIGHTS_INSTRUMENTATIONKEY'])

url = os.environ["COSMOS_URI"]
key = os.environ["COSMOS_KEY"]
client = CosmosClient(url, key)

database = client.get_database_client(os.environ["COSMOS_DATABASE_NAME"])
container = database.get_container_client(os.environ["COSMOS_CONTAINER_NAME"])

def main(ingestion: List[func.EventHubEvent]) -> None:
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("ingest_items"):
        logging.info(f"Ingested {len(ingestion) - 1} events.")
        
        cosmos_items: List[CosmosItem] = []
        for raw_item in ingestion:
            item = json.loads(raw_item.get_body().decode('utf-8'), object_hook=lambda d: Item(**d))
            cosmos_items.append(CosmosItem(id=str(item.id), order_id=str(item.order_id), description=item.description, price=item.price))
            
        for cosmos_item in cosmos_items:
            container.upsert_item(body=cosmos_item.model_dump())
