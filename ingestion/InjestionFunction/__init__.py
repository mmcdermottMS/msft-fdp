from collections import defaultdict
import logging
import os
from typing import List

import azure.functions as func
from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.propagate import extract

#root_logger = logging.getLogger()
#for handler in root_logger.handlers[:]:
#    root_logger.removeHandler(handler)

configure_azure_monitor()

from SharedCode.Item import Item
from SharedCode.Order import Order

CONNECTION_STR = os.environ['EHNS_CONN_STRING']
EVENTHUB_NAME = os.environ['TARGET_EH_NAME']
PARTITION_COUNT = int(os.environ['PARTITION_COUNT'])
MAX_BATCH_SIZE_IN_BYTES = 1048576

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def main(ingestion: List[func.EventHubEvent], context) -> None:
    #https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry#monitoring-in-azure-functions
    carrier = {
        "traceparent": context.trace_context.Traceparent,
        "tracestate": context.trace_context.Tracestate,
    }
    
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("ingest_items", context=extract(carrier)):
        logging.info(f"Ingested {len(ingestion)} events.")
        items: List[Item] = []
        for raw_order in ingestion:
            order = Order.model_validate_json(raw_order.get_body().decode('utf-8'))
            for item in order.items:
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
                logging.info(F"Ingest: Published {len(event_data_batch)} items in a batch")
                
                event_data_batch = producer.create_batch(max_size_in_bytes=MAX_BATCH_SIZE_IN_BYTES, partition_key=str(partition_key))
                
                try:
                    event_data_batch.add(EventData(str(item.model_dump())))
                except ValueError:
                    logging.error("Message too large to fit into EventDataBatch object")

        producer.send_batch(event_data_batch)
        logging.info(F"Ingest: Published {len(event_data_batch)} items to partition key {partition_key}")