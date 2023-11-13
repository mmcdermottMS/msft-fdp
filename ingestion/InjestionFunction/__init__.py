from collections import defaultdict
import json
import logging
import os
from typing import List

import azure.functions as func
from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace

from SharedCode.Item import Item

CONNECTION_STR = os.environ['EHNS_CONN_STRING']
EVENTHUB_NAME = os.environ['TARGET_EH_NAME']
PARTITION_COUNT = int(os.environ['PARTITION_COUNT'])
MAX_BATCH_SIZE_IN_BYTES = 1048576

configure_azure_monitor(connection_string=os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'], service_name="GenerationFunction", instrumentation_key=os.environ['APPINSIGHTS_INSTRUMENTATIONKEY'])
producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def main(ingestion: List[func.EventHubEvent]) -> None:
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("ingest_items"):
        logging.info(f"Ingested {len(ingestion) - 1} events.")
        
        items: List[Item] = []
        for raw_item in ingestion:
            item = json.loads(raw_item.get_body().decode('utf-8'), object_hook=lambda d: Item(**d))
            items.append(item)

        publish(items)
        
        
def publish(items: List[Item]):
    
    items_by_order_id = defaultdict(list)
    for item in items:
        items_by_order_id[item.order_id % PARTITION_COUNT].append(item)
        
    for partition_key, items in items_by_order_id.items():        
        event_data_batch = producer.create_batch(max_size_in_bytes=MAX_BATCH_SIZE_IN_BYTES, partition_key=str(partition_key))
        
        for item in items:
            try:
                event_data_batch.add(EventData(str(item.model_dump())))
            except ValueError:
                producer.send_batch(event_data_batch)
                
                event_data_batch = producer.create_batch(max_size_in_bytes=MAX_BATCH_SIZE_IN_BYTES, partition_key=str(partition_key))
                
                try:
                    event_data_batch.add(EventData(str(item.model_dump())))
                except ValueError:
                    logging.error("Message too large to fit into EventDataBatch object")

        producer.send_batch(event_data_batch)