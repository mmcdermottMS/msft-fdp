from typing import List
import json
import os
import azure.functions as func
import logging
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace

from SharedCode.Item import Item

configure_azure_monitor(connection_string=os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'], service_name="TransformationFunction", instrumentation_key=os.environ['APPINSIGHTS_INSTRUMENTATIONKEY'])

def main(ingestion: List[func.EventHubEvent]) -> None:
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("ingest_items"):
        logging.info(f"Ingested {len(ingestion) - 1} events.")
        
        items: List[Item] = []
        for raw_item in ingestion:
            item = json.loads(raw_item.get_body().decode('utf-8'), object_hook=lambda d: Item(**d))
            items.append(item)