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
