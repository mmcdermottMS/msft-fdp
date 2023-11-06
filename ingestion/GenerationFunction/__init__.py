from collections import defaultdict
import logging
import os
import random
from typing import List

import azure.functions
from azure.eventhub import EventData
from azure.eventhub import EventHubProducerClient

from SharedCode.Item import Item

CONNECTION_STR = os.environ['EHNS_CONN_STRING']
EVENTHUB_NAME = os.environ['EH_NAME']

producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

def main(req: azure.functions.HttpRequest) -> azure.functions.HttpResponse:
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
    
    partitionCount = 3 #TODO: parameterize the partition count
    max_size_in_bytes = 1048576 #TODO: parameterize the max size in bytes
    items_by_order_id = defaultdict(list)
    for item in items:
        items_by_order_id[item.order_id % partitionCount].append(item)
        
    for partitionKey, items in items_by_order_id.items():        
        event_data_batch = producer.create_batch(max_size_in_bytes=max_size_in_bytes, partition_key=str(partitionKey))
        
        for item in items:
            try:
                event_data_batch.add(EventData(str(item.model_dump())))
            except ValueError:
                producer.send_batch(event_data_batch)
                
                event_data_batch = producer.create_batch(max_size_in_bytes=max_size_in_bytes, partition_key=str(partitionKey))
                
                try:
                    event_data_batch.add(EventData(str(item.model_dump())))
                except ValueError:
                    logging.error("Message too large to fit into EventDataBatch object")

        producer.send_batch(event_data_batch)
    