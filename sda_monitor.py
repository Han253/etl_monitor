"""
SDA (Self Description Adaptare)

Modulo para monitorear y reportar cambios en la infraestructura software, se encarga de recibir
los cambios en el dispositivo monitoreado y generar una novedad con el detalle para su procesamiento

Autor: Henry Jiménez
Version: 1.0

"""

#Import Libraries
import schedule
import time, sys, os
import pika
from datetime import datetime
import json
from tinydb import TinyDB
from redisTool import RedisQueue
import typer
import logging

from random import random

#Typer App CLI Helper
app = typer.Typer()

#Log File
logger = logging.getLogger('sda_monitor')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('register.log')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

#Self-Description Adapter
class SDA():

    ITEMS_TYPES = ["property","resource","device","app","device_resource","app_device"]
    QUEUE_TYPES = ["create","update","delete"]

    def __init__(self,file_data_name='last_status.json',amqp_host='localhost',queue='representation',interval=5):
        self.file_data_name = file_data_name
        self.amqp_host = amqp_host
        self.queue = queue
        self.interval = interval
        self.db = TinyDB('db.json')
        self.create_queue = RedisQueue("register")        
     
    
    def get_last_data(self):
        with open(self.file_data_name) as f:
            self.last_data = json.load(f)
    
    def init_SDA_loop(self):
        #Programing task
        schedule.every(self.interval).seconds.do(self.sda_monitor)
        print(' [*] Start sending messages. To exit press CTRL+C')
        #Principal bucle
        while True:
            #Check a shedule task is pending to run or not
            schedule.run_pending()
            time.sleep(self.interval)
    
    def set_devices(self,list,devices,device_parent):
        if devices!= None and len(devices)!=0:
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
    
    #Get devices List
    def get_devices_list(self,input_device=None,input_list=[]):
        output_list=[]       
        #Gateway devices
        if input_device!=None:
            i_device = input_device.copy()
            device_list = i_device.pop("devices",None)
            output_list.append(i_device)
        else:
            device_list = input_list
        for device in device_list:
            i_device = device.copy()
            sub_devices_list = i_device.pop("devices",None)
            output_list.append(i_device)
            if sub_devices_list!=None and len(sub_devices_list) !=0:
                output_sub_list = self.get_devices_list(input_device=None,input_list=sub_devices_list)        
                output_list = output_list+output_sub_list
        return output_list
    

    #Define extract data process
    def extract_data(self):
        #Get notifications
        data_to_transform = []

        while not self.create_queue.empty():
            data_to_transform.append(json.loads(self.create_queue.get().decode('UTF-8')))
                        
        return data_to_transform
        
    #Define transformation data process
    def transformation(self,data):

        data_response = {}
        send_response = False

        if len(data)>0:
            #Create data response structure
            for queue in self.QUEUE_TYPES:
                data_response[queue] = {}
                for item in self.ITEMS_TYPES:
                    data_response[queue][item] = []
        
            #Get data response
            for item in data:
                send_response = True
                data_response[item["queue"]][item["type"]].append(item["content"])
                time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                str_log = "" + item["queue"].upper()+":"
                str_log += " " + item["type"].upper()
                if "global_id" in item["content"]:
                    str_log += " G_ID:"+str(item["content"]["global_id"])
                elif "name" in item["content"]:
                    str_log += " NAME:"+str(item["content"]["name"])
                    str_log += " PROP_TYPE:"+str(item["content"]["prop_type"])
                    str_log += " PARENT_ID:"+str(item["content"]["parent_id"])
                elif "resource_id" in item["content"]:
                    str_log += " DEVICE:"+str(item["content"]["device_id"])
                    str_log += " RESOURCE:"+str(item["content"]["resource_id"])
                elif "app_id" in item["content"]:
                    str_log += " APP:"+str(item["content"]["app_id"])
                    str_log += " DEVICE:"+str(item["content"]["device_id"])
                str_log += " " + time
                logger.info(str_log)

        if send_response:
            data_response["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            table = self.db.table('logs')
            table.insert(data_response)
            json_data =  json.dumps(data_response)
        else:
            json_data = None

        return json_data  


       
        """
        #firt time to save representation
        if self.last_data == {}:
            with open(self.file_data_name, 'w') as f:
                json.dump(data, f)
                print("json file was update")
            if data!= None:
                data_response["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                device_data = data["device"]
                apps_data = data["apps"]
                resources_data = data["resources"]

                #Resources
                for resource in resources_data:
                    data_response["CREATE"]["resources"].append(resource)

                #Devices
                devices_list = device_data.pop("devices",None)
                data_response["CREATE"]["devices"].append(device_data)                
                self.set_devices(data_response["CREATE"]["devices"],devices_list,device_data["global_id"])
                
                #Applications
                for app in apps_data:
                    data_response["CREATE"]["apps"].append(app)               
                j_response = json.dumps(data_response)
        
        #Compare last representation with the new
        else:
            send_response = False     
            
            #Resources
            last_resources_list = self.last_data["resources"]
            last_resources_list_id = [x["global_id"] for x in last_resources_list]
            new_resources_list = data["resources"]
            for resource in new_resources_list:
                if resource["global_id"] in last_resources_list_id:
                    index = last_resources_list_id.index(resource["global_id"])
                    last_resource = last_resources_list[index]
                    if resource != last_resource:
                        #Old resources with changes
                        send_response = True
                        data_response["UPDATE"]["resources"].append(resource)

                    last_resources_list.remove(last_resource)
                    last_resources_list_id.pop(index)
                else:
                    #new resource
                    send_response = True
                    data_response["CREATE"]["resources"].append(resource)

            if len(last_resources_list)>0:
                send_response = True
                for del_resource in last_resources_list:
                    data_response["DELETE"]["resources"].append(del_resource)  

            #Devices
            last_devices_list = self.get_devices_list(input_device=self.last_data["device"])            
            last_devices_list_id = [x["global_id"] for x in last_devices_list]
            new_devices_list = self.get_devices_list(input_device=new_data["device"])            
            for device in new_devices_list:
                #Old Devices
                if device["global_id"] in last_devices_list_id:
                    index = last_devices_list_id.index(device["global_id"])
                    last_device = last_devices_list[index]
                    if device != last_device:
                        #Old devices with changes
                        #device.pop("devices",None)--DELETE
                        send_response = True
                        data_response["UPDATE"]["devices"].append(device)

                    last_devices_list.remove(last_device)
                    last_devices_list_id.pop(index)
                        
                else:
                    #new device
                    send_response = True
                    data_response["CREATE"]["devices"].append(device)                
            
            if len(last_devices_list)>0:
                send_response = True
                for del_device in last_devices_list:
                    data_response["DELETE"]["devices"].append(del_device)

            #Applications
            last_apps_list = self.last_data["apps"]
            last_apps_list_id = [x["global_id"] for x in last_apps_list]
            new_apps_list = data["apps"]
            for app in new_apps_list:
                if app["global_id"] in last_apps_list_id:
                    index = last_apps_list_id.index(app["global_id"])
                    last_app = last_apps_list[index]
                    if app != last_app:
                        #Old apps with changes
                        send_response = True
                        data_response["UPDATE"]["apps"].append(app)

                    last_apps_list.remove(last_app)
                    last_apps_list_id.pop(index)
                else:
                    #new app
                    send_response = True
                    data_response["CREATE"]["apps"].append(app)

            if len(last_apps_list)>0:
                send_response = True
                for del_app in last_apps_list:
                    data_response["DELETE"]["apps"].append(del_app)

            if send_response:
                with open(self.file_data_name, 'w') as f:                    
                    json.dump(data, f)
                    print("json file was update")
                data_response["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                j_response = json.dumps(data_response)
            else:
                j_response = None
        return j_response
    """

    #Define load data process
    def load(self,data):
        if data!= None:
            self.send_message(data)
        else:
            print("[*] Not changes")

    #Define SDA Monitor.
    def sda_monitor(self):
        start_time = time.time()
        data = self.extract_data()        
        data = self.transformation(data)
        self.load(data)
        end_time = time.time()
        print("---%s seconds ---" % (end_time-start_time))


#Main Task
@app.command()
def main(interval: int = 5):
    adapter = SDA(interval=interval)
    adapter.init_SDA_loop()

if __name__ == '__main__':
    try:
        app()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)