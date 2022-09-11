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
        amqp_broker_host = os.getenv("AMQP_BROKER", default='localhost')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_broker_host))
        channel = connection.channel()
        channel.queue_declare(queue='representation')
        channel.basic_publish(exchange='', routing_key='representation', body=message)
        #print(" [X] Send data -- %s" % message)
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

        while not self.create_queue.empty():
            notification = json.loads(self.create_queue.get().decode('UTF-8'))
            self.transformation([notification])
        
        
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
            json_data =  json.dumps(data_response)
        else:
            json_data = None

        self.load(json_data)

    #Define load data process
    def load(self,data):
        if data!= None:
            self.send_message(data)
        else:
            pass
            #print("[*] Not changes")

    #Define SDA Monitor.
    def sda_monitor(self):
        #start_time = time.time()
        self.extract_data()        
        #data = self.transformation(data)
        #self.load(data)
        #end_time = time.time()
        #print("---%s seconds ---" % (end_time-start_time))


#Main Task
@app.command()
def main(interval: int = 5):
    adapter = SDA(interval=interval, amqp_host='192.168.1.105')
    adapter.init_SDA_loop()

if __name__ == '__main__':
    try:
        app()
    except KeyboardInterrupt:
        #print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)