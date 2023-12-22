import logging
import os
import random
from typing import List

import azure.functions
from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient

#Import the Azure Open Telemetry and native Open Telemetry modules
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
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

CONNECTION_STR = os.environ['EHNS_CONN_STRING']
EVENTHUB_NAME = os.environ['EH_NAME']
PARTITION_COUNT = int(os.environ['PARTITION_COUNT'])
MAX_BATCH_SIZE_IN_BYTES = 1048576

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def main(req: azure.functions.HttpRequest, context) -> azure.functions.HttpResponse:

    #From https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry#monitoring-in-azure-functions
    #Capture the OTel traceparent and tracestate from the current execution context, these will be passed along in the downstream calls
    carrier = {
      "traceparent": context.trace_context.Traceparent,
      "tracestate": context.trace_context.Tracestate,
    }
    
    #Grab a handle to the current tracer and start a new span, but pass in the details captured above from the current TraceContext
    tracer = trace.get_tracer(__name__)    
    with tracer.start_as_current_span("generate_items", context=extract(carrier)): #Give the span a unique name, it will show up in App Insights as a parent dependency to all calls made within the span
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

        orders = []
        for x in range(message_count):
            order_id = random.randint(1, 1000)
            item_count = random.randint(1, 100)
            order_items = []
            for y in range(item_count):
                id = random.randint(1, 1000000)
                price = random.uniform(0, 1000)
                order_item = Item(id=id, order_id=order_id, description=f"Item {y} on Order {x}", price=price)
                order_items.append(order_item)
            order = Order(id=order_id, items=order_items)
            orders.append(order)
        
        publish(orders)
        
        return azure.functions.HttpResponse(f"Generate: {message_count_str} events sent to Event Hub.", status_code=200)


def publish(orders: List[Order]):

    for order in orders:
        #Explicitly setting the partition key
        event_data_batch = producer.create_batch(max_size_in_bytes=MAX_BATCH_SIZE_IN_BYTES, partition_key=str(order.id))
        
        try:
            event_data_batch.add(EventData(str(order.model_dump())))
        except ValueError:
            producer.send_batch(event_data_batch)
            logging.info(F"Generate: Published {len(event_data_batch)} orders in a batch. (Exception)")
            
            event_data_batch = producer.create_batch(max_size_in_bytes=MAX_BATCH_SIZE_IN_BYTES, partition_key=str(order.id))
            
            try:
                event_data_batch.add(EventData(str(order.model_dump())))
            except ValueError:
                logging.error("Generate: Message too large to fit into EventDataBatch object")

        producer.send_batch(event_data_batch)
        logging.info(F"Generate: Published {len(event_data_batch)} orders in a batch")