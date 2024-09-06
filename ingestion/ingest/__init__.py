from collections import defaultdict
import logging
import os
from typing import List

import azure.functions as func
from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient

#Import the Azure Open Telemetry and native Open Telemetry modules
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry import metrics
from opentelemetry.propagate import extract

from SharedCode.Item import Item
from SharedCode.Order import Order

#Wire up and initialize the Azure Monitor components.
#You can pass in the connection string to the method, or define it as an environment variable/app setting called APPLICATIONINSIGHTS_CONNECTION_STRING.
#See https://learn.microsoft.com/en-us/python/api/azure-monitor-opentelemetry/azure.monitor.opentelemetry?view=azure-python for more details

#WARNING: the following line should be run once and only once PER FUNCTION APP.  
#If you have more than 1 function deployed to a given function app, the recommendation is to wrap this
#in thread-safe code using a lock object or similar so that only a single function within the function app
#executes this line.  If this line runs more than once, you will see duplicate dependency records in
#Application Insights, one for every instance that this line was called
configure_azure_monitor()


#For capturing custom metrics, first define a global meter object:
meter = metrics.get_meter_provider().get_meter("ingestion_meter")
#Next create one or more instruments to capture the metrics you want to track.  In this case, we're tracking the number of items ingested
itemCounter = meter.create_counter(name="order_item_counter", description="Item Counter", unit="1")
#We can also create a second instrument to track the number of orders
orderCounter = meter.create_counter(name="order_counter", description="Order Counter", unit="1")
#These instruments should be created once and only once, and then used throughout the function to capture the metrics, they can be
#global variables and passed around as needed


CONNECTION_STR = os.environ['EHNS_CONN_STRING_TARGET']
EVENTHUB_NAME = os.environ['TARGET_EH_NAME']
PARTITION_COUNT = int(os.environ['PARTITION_COUNT'])
MAX_BATCH_SIZE_IN_BYTES = 1048576

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def main(ingestion: List[func.EventHubEvent], context) -> None:

    #From https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry#monitoring-in-azure-functions
    #Capture the OTel traceparent and tracestate from the current execution context, these will be passed along in the downstream calls
    carrier = {
        "traceparent": context.trace_context.Traceparent,
        "tracestate": context.trace_context.Tracestate,
    }
    
    #Grab a handle to the current tracer and start a new span, but pass in the details captured above from the current TraceContext
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("ingest_items", context=extract(carrier)): #Give the span a unique name, it will show up in App Insights as a parent dependency to all calls made within the span
        logging.info(f"Ingest: Ingested {len(ingestion)} events.")
        items: List[Item] = []
        for raw_order in ingestion:
            orderCounter.add(1) #Increment the order counter by 1 for each order ingested
            order = Order.model_validate_json(raw_order.get_body().decode('utf-8'))
            for item in order.items:
                itemCounter.add(1) #Increment the item counter by 1 for each item in the order
                items.append(item)

        publish(items)        
        
        
def publish(items: List[Item]):    
    items_by_order_id = defaultdict(list)
    for item in items:
        items_by_order_id[item.order_id % PARTITION_COUNT].append(item)
        
    for partition_key, items in items_by_order_id.items():
        #Explicitly setting the partition key based on Order ID to ensure that all items within an order go to the same Event Hub
        #partition to guarantee they are processed in the correct order        
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
                    logging.error("Ingest: Message too large to fit into EventDataBatch object")

        producer.send_batch(event_data_batch)
        logging.info(F"Ingest: Published {len(event_data_batch)} items to partition key {partition_key}")