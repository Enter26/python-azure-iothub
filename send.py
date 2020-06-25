import random  
import time  
import env
import json
from datetime import datetime
from azure.iot.device import IoTHubDeviceClient, Message  
  
CONNECTION_STRING = env.Iot_Hub_connection_string   
  

def prepare_measure():

    #pobranie aktualnej daty i godziny          
    now = datetime.now()
    now = str(now)
    data1 = random.randint(0,100)
    data2 = random.randint(0,50)
    data3 = random.randint(0,75)
 
    x = { "date_time":now,
    "data1": data1,
    "data2": data2,
    "data3": data3
    }

    return json.dumps(x)



def iothub_client_init():  
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)  
    return client  
  
def iothub_client_telemetry_sample_run():  
  
    try:  
        client = iothub_client_init()  
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )  
        while True:  
            
            message = prepare_measure()
  
            print( "Sending message: {}".format(message) )  
            client.send_message(message)  
            print ( "Message successfully sent" )  
            time.sleep(10)  
  
    except KeyboardInterrupt:  
        print ( "IoTHubClient sample stopped" )  
  
if __name__ == '__main__':  
    print ( "IoT Hub Quickstart #1 - Simulated device" )  
    print ( "Press Ctrl-C to exit" )  
    iothub_client_telemetry_sample_run()  
