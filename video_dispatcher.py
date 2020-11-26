from datetime import datetime
from multiprocessing.connection import Client,Listener
from util.SetInterval import setInterval
from optimal_downsampling_manager.resource_predictor.table_estimator import get_context,IATable,drop_measurement_if_exist

from influxdb import InfluxDBClient
from pathlib import Path
import yaml
import threading
import time
import os
import csv

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)



class Dispatcher():
    def __init__(self):
        pass
         
    @setInterval(1)
    def check_ready(self):
        try:
            IAE_ready = self.conn_send2IAE 
            AP_ready = self.conn_listen2AP
            DDM_ready = self.conn_send2DDM
            DP_ready = self.conn_listen2DP
            if DDM_ready and DP_ready:
                self.ready.set()
        except Exception as e:
            print(e)
        

    #set port, run monitor    
    def run(self):
        self.ready.wait() #wait every port set up
        print("[INFO] Camera is running the task")
        try:
            self.start_IAE_workload()
            self.start_DDM_workload()
            input("Start to dispatching workload...")
            print()
        except Exception as e:    
            print(e)
    

    # @setInterval(trigger_interval*3600)
    def do(self):
        while True:
            time.sleep(1) # check every second
            now = datetime.now()
            if ((now.hour-self.hour + 24) % 24)* 60 + (now.minute-self.minute) > data['dispatcher']['IAE_trigger_time'] * 60:
                self.hour = now.hour
                self.minute 
            else:
                continue
            t = threading.Thread(target=self.gen_workload)
            t.start()
            t.join()
    
    def open_DDM_sending_port(self):
        while True:
            time.sleep(1)
            try:
                if self.conn_send2DDM is None:
                    address = ('localhost',3000)
                    self.conn_send2DDM = Client(address)
                    print("[INFO] Connected with DDM...")
            except Exception as e:
                print("Not detecting DDM, reconnecting...")


    def open_IAE_sending_port(self):
        while True:
            time.sleep(1)
            try:
                if self.conn_send2IAE is None:
                    address = ('localhost',5000)
                    self.conn_send2IAE = Client(address)
                    print("[INFO] Connected with IAE...")
            except Exception as e:
                print("Not detecting IAE, reconnecting...")

    ## this port is for simulation
    def open_AP_listening_port(self):
        address = ('localhost',6000)
        listener = Listener(address)
        while True:
            time.sleep(1)
            try:
                if self.conn_listen2AP is None:
                    print("[INFO] Listening from Analytic Platform")
                    self.conn_listen2AP = listener.accept()
                    print('connection accepted from', listener.last_accepted)

            except Exception as e:
                print("[INFO] Close listening port")
                self.conn_listen2AP.close()  
                self.conn_listen2AP = None  
                print(e)

    ## this port is for simulation
    def open_DP_listening_port(self):
        address = ('localhost',6001)
        listener = Listener(address)
        while True:
            time.sleep(1)
            try:
                if self.conn_listen2DP is None:
                    print("[INFO] Listening from Downsampling Platform")
                    self.conn_listen2DP = listener.accept()
                    print('connection accepted from', listener.last_accepted)

            except Exception as e:
                print("[INFO] Close listening port")
                self.conn_listen2DP.close()  
                self.conn_listen2DP = None  
                print(e)
    
    def start_IAE_workload(self):
        try: 
            result = self.DBclient.query("SELECT * FROM raw_videos WHERE \"status\"=\"unprocessed\""])
            result_list = list(result.get_points(measurement="raw_videos"))
            for r in result_list:
                json_body = [
                        {
                            "measurement": "pending_video",
                            "tags": {
                                "name":r['name'],
                            },
                            "fields": {
                                "host": "webcamPole8"
                            }
                        }
                    ]
                self.DBclient.write_points(json_body)

            self.conn_send2IAE.send(True)
            print("Send signal to IAE")
        except Exception as e:
            print(e)


    def start_DDM_workload(self):
        try:
            
        except Exception as e:


if __name__=="__main__":
    print(data['global']['ipcamlist'])

