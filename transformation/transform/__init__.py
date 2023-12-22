import logging
import os
from typing import List

import azure.functions as func
from azure.cosmos import CosmosClient

#Import the Azure Open Telemetry and native Open Telemetry modules
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.propagate import extract

from SharedCode.CosmosItem import CosmosItem
from SharedCode.Item import Item

#Wire up and initialize the Azure Monitor components.
#You can pass in the connection string to the method, or define it as an environment variable/app setting called APPLICATIONINSIGHTS_CONNECTION_STRING.
#See https://learn.microsoft.com/en-us/python/api/azure-monitor-opentelemetry/azure.monitor.opentelemetry?view=azure-python for more details

#WARNING: the following line should be run once and only once PER FUNCTION APP.  
#If you have more than 1 function deployed to a given function app, the recommendation is to wrap this
#in thread-safe code using a lock object or similar so that only a single function within the function app
#executes this line.  If this line runs more than once, you will see duplicate dependency records in
#Application Insights, one for every instance that this line was called
configure_azure_monitor()

client = CosmosClient(os.environ["COSMOS_URI"], os.environ["COSMOS_KEY"])
database = client.get_database_client(os.environ["COSMOS_DATABASE_NAME"])
container = database.get_container_client(os.environ["COSMOS_CONTAINER_NAME"])

def main(transformation: List[func.EventHubEvent], context) -> None:

    #From https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/monitor/azure-monitor-opentelemetry#monitoring-in-azure-functions
    #Capture the OTel traceparent and tracestate from the current execution context, these will be passed along in the downstream calls
    carrier = {
        "traceparent": context.trace_context.Traceparent,
        "tracestate": context.trace_context.Tracestate,
    }
    
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("transform_items", context = extract(carrier)): #Give the span a unique name, it will show up in App Insights as a parent dependency to all calls made within the span
        cosmos_items: List[CosmosItem] = []
        for raw_item in transformation:
            item = Item.model_validate_json(raw_item.get_body().decode('utf-8'))

            #Simulate filtering of inbound messages.  If Item ID is divisible by 7, ignore it
            if item.id % 7 != 0:
                cosmos_items.append(CosmosItem(id=str(item.id), order_id=str(item.order_id), description=item.description, price=item.price))

        logging.info(f"Transform: Transformed {len(cosmos_items)} items.")
        for cosmos_item in cosmos_items:
            container.upsert_item(body=cosmos_item.model_dump())
