from collections import defaultdict
import logging
import os
import random
from typing import List

import azure.functions
from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace

from SharedCode.Item import Item

CONNECTION_STR = os.environ['EHNS_CONN_STRING']
EVENTHUB_NAME = os.environ['SOURCE_EH_NAME']
PARTITION_COUNT = int(os.environ['PARTITION_COUNT'])
MAX_BATCH_SIZE_IN_BYTES = 1048576

#https://learn.microsoft.com/en-us/azure/azure-monitor/app/opentelemetry-enable?tabs=python
configure_azure_monitor(connection_string=os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'], service_name="GenerationFunction", instrumentation_key=os.environ['APPINSIGHTS_INSTRUMENTATIONKEY'])

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def main(req: azure.functions.HttpRequest) -> azure.functions.HttpResponse:
    # Get a tracer for the current module.
    tracer = trace.get_tracer(__name__)

    # Start a new span with the name "hello". This also sets this created span as the current span in this context. This span will be exported to Azure Monitor as part of the trace.
    with tracer.start_as_current_span("generate_items"):
        message_count_str = req.params.get('messageCount')
        if not message_count_str:
            try:
                req_body = req.get_json()
            except ValueError:
                message_count_str = "1"
            else:
                message_count_str = req_body.get('messageCount')
                
        try: 
            message_count: int = int(message_count_str)
        except ValueError:
            message_count: int = 1

        order_items = []
        for x in range(message_count):
            id = random.randint(1, 1000000)
            order_id = random.randint(1, 300)
            price = random.uniform(0, 1000)
            order_item = Item(id=id, order_id=order_id, description=f"Item {x}", price=price)
            order_items.append(order_item)
        
        publish(order_items)
        
        return azure.functions.HttpResponse(f"{message_count_str} events sent to Event Hub.", status_code=200)
    
    
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
    