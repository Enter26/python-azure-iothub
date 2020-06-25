import json
import time
import psycopg2
import env
from azure.eventhub import EventHubClient, Receiver, Offset

ADDRESS = env.event_hub_address

USER = env.event_hub_user
KEY = env.event_hub_key

CONSUMER_GROUP = env.consumer_group
OFFSET = Offset(env.offset)
PARTITION = env.partition

total_receive = 0
total_send = 0
last_sn = -1
last_offset = "-1"

prepare_query = "INSERT INTO data (data1, data2, data3, data_time) VALUES(%s,%s,%s,%s)"


client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
try:
    print("connected")
    receiver = client.add_receiver(
        CONSUMER_GROUP, PARTITION, prefetch=5000, offset=OFFSET)
    client.run()
    start_time = time.time()
    for event_data in receiver.receive(timeout=100):
        y = json.loads(event_data.body_as_str(encoding="UTF-8"))
        print(y)
        total_receive += 1

        values_to_insert = (y["data1"], y["data2"], y["data3"], y["data_time"])
        try:
            conn = psycopg2.connect(host=env.db_host, port=env.db_port,database=env.db_name,
                    user=env.db_user, password=env.db_password)
            cur = conn.cursor()
            cur.execute(prepare_query, values_to_insert)
            print("Insert query completed")

            conn.commit()
            conn.close()
            total_send += 1
        except Exception as e:
            print("Exception - {}".format(e))

            
    end_time = time.time()
    client.stop()
    run_time = end_time - start_time
    print("Received {} mesages, send to database {} messages in {} seconds".format(total_receive,
        total_send, run_time))

except KeyboardInterrupt:
    pass
finally:
    client.stop()













#import logging
#from azure.eventhub import EventHubConsumerClient

#connection_str = 'Endpoint=sb://ihsuprodblres100dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=mK9zFsKY6jG+Fbao5/6DCyM7ZC+83zkpncIxTJ+0tps=;EntityPath=iothub-ehub-iot-test-d-3522519-10cc10fcb3'
#consumer_group = 'iot-test-device-consume'
#eventhub_name = 'iothub-ehub-iot-test-d-3522519-10cc10fcb3'
#client = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=eventhub_name)

#logger = logging.getLogger("azure.eventhub")
#logging.basicConfig(level=logging.INFO)

#def on_event(partition_context, event):
#    logger.info("Received event from partition {}".format(partition_context.partition_id))
#    partition_context.update_checkpoint(event)
#    message = partition_context.receive_message(event)
#    print(event)
#    print(message)

#with client:
#    print(client.receive(on_event=on_event))


#with client:
#    client.receive(
 #       on_event=on_event, 
 #       starting_position="-1",  # "-1" is from the beginning of the partition.
 #   )
    # receive events from specified partition:
#     msg=client.receive(on_event=on_event, partition_id='1')
#     print(msg)
