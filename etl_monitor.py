#Import Libraries
import schedule
import time, sys, os
import pika
from datetime import datetime
import json
import requests

from random import random

class ETLMonitor():

    def __init__(self,file_data_name='last_status.json',amqp_host='localhost',queue='representation',interval=10):
        self.file_data_name = file_data_name
        self.amqp_host = amqp_host
        self.queue = queue
        self.interval = interval
        self.DATA_URL = "http://localhost:5000/device/api/1"
        self.last_data = None

    def init_data_file(self):
         #Validate temporal data
        if not os.path.exists(self.file_data_name):
            with open(self.file_data_name,'w') as f:
                json.dump({}, f)
    
    def get_last_data(self):
        with open(self.file_data_name) as f:
            self.last_data = json.load(f)
    
    def init_ETL_loop(self):
        #Programing task
        schedule.every(self.interval).seconds.do(self.etl_monitor)
        print(' [*] Start sending messages. To exit press CTRL+C')
        #Principal bucle
        while True:
            #Check a shedule task is pending to run or not
            schedule.run_pending()
            time.sleep(self.interval)
    
    def set_devices(self,list,devices,device_parent):
        if len(devices)!=0:
            for device in devices:
                devices_list = device.pop("devices",None)
                device["device_parent"] = device_parent
                list.append(device)
                self.set_devices(list,devices_list,device["global_id"])

    #Send AMQP Broker data
    def send_message(self,message):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='representation')
        channel.basic_publish(exchange='', routing_key='representation', body=message)
        print(" [X] Send data -- %s" % message)
        connection.close()

    #Define extract data process
    def extract_data(self):
        data_rq = requests.get(self.DATA_URL)
        data_dict= json.loads(data_rq.text)        
        return data_dict
    
    #Get devices List
    def get_devices_list(self,input_list,output_list=[]):
        for device in input_list:
            sub_devices_list = device.pop("devices",None)
            output_list.append(device)
            if sub_devices_list!=None and len(sub_devices_list) !=0:
                output_list = self.get_devices_list(sub_devices_list,output_list)        
        return output_list
    
    #Define transformation data process
    def transformation(self,data):
        self.get_last_data()

        data_response = {}
        data_response["CREATE"] = {"devices":[]}
        data_response["UPDATE"] = {"devices":[]}
        data_response["DELETE"] = {"devices":[]}
        
        #firt time to save representation
        if self.last_data == {}:
            with open(self.file_data_name, 'w') as f:
                json.dump(data, f)
                print("json file was update")
            if data!= None:
                devices_list = data.pop("devices",None)
                data_response["CREATE"]["devices"].append(data)
                data_response["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.set_devices(data_response["CREATE"]["devices"],devices_list,data["global_id"])
                j_response = json.dumps(data_response)
        
        #Compare last representation with the new
        else:
            send_response = False            
            data_response["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            last_devices_list = self.get_devices_list(self.last_data["devices"],[])
            last_devices_list_id = [x["global_id"] for x in last_devices_list]           
            new_devices_list = self.get_devices_list(data["devices"],[])
            for device in new_devices_list:
                #Old Devices
                if device["global_id"] in last_devices_list_id:
                    index = last_devices_list_id.index(device["global_id"])
                    last_device = last_devices_list[index]
                    if device != last_device:
                        #Old devices with changes
                        device.pop("devices",None)
                        send_response = True
                        data_response["UPDATE"]["devices"].append(device)

                    last_devices_list.remove(last_device)
                    last_devices_list_id.pop(index)
                        
                else:
                    #new device
                    send_response = True
                    data_response["DELETE"]["devices"].append(device)                
            
            if len(last_devices_list)>0:
                send_response = True
                for del_device in last_devices_list:
                    data_response["DELETE"]["devices"].append(del_device) 


            if send_response:
                with open(self.file_data_name, 'w') as f:
                    json.dump(data, f)
                    print("json file was update")
                j_response = json.dumps(data_response)
            else:
                j_response = None
        return j_response

    #Define load data process
    def load(self,data):
        if data!= None:
            self.send_message(data)
        else:
            print("[*] Not changes")

    #Define ETL Monitor.
    def etl_monitor(self):
        data = self.extract_data()
        data = self.transformation(data)
        self.load(data)


#Main Task
def main():
    etl_monitor = ETLMonitor()
    etl_monitor.init_data_file()
    etl_monitor.init_ETL_loop()   
    

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


