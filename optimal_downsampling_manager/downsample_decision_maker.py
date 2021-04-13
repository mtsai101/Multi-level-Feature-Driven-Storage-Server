from multiprocessing.connection import Client,Listener
from .decision_type import Decision
from .P_generator import generate_P
from influxdb import InfluxDBClient
from util.SetInterval import setInterval
import numpy as np
import threading
import random
import time
import math
import yaml

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

class DownSampleDecisionMaker(object):
    def __init__(self):
        self.conn_send2DP = None
        self.conn_listenDBAgent = None

        self.ready = threading.Event()
        self.DBclient = InfluxDBClient(host=data['global']['database_ip'], port=data['global']['port'], database=data['global']['database_name'], username='root', password='root')
        self.DBAlistener = Listener(('localhost',int(data['global']['agent2DDM'])))


    def open_DBA_listening_port(self):
        while True:
            time.sleep(3)
            try:
                if self.conn_listenDBAgent is None:
                    print("[INFO] Listening from DB Agent")
                    self.conn_listenDBAgent = self.DBAlistener.accept()
                    print('connection accepted from', self.DBAlistener.last_accepted)

            except Exception as e:
                print("[INFO] Close listening port")
                self.conn_listenDBAgent.close()  
                self.conn_listenDBAgent = None  
                print(e)
                
    def open_DP_sending_port(self):
        while True:
            time.sleep(1)
            try:
                if self.conn_send2DP is None:
                    address = ('localhost',int(data['global']['DDM2DP']))
                    self.conn_send2DP = Client(address)
                    print("Connected with Down Platform")
            except Exception as e:
                print("Not detecting Down Platform, reconnecting...")
    
    @setInterval(1)
    def check_ready(self):
        if self.conn_send2DP and self.conn_listenDBAgent:
            self.ready.set()
    
    #set port, run monitor    
    def run(self):
        self.ready.wait() #wait every port set up
        print("[INFO] DDM is running the task")
        try:
            stopper = self.do()
            input("Press any key to stop...")
            print("")
        except Exception as e:    
            print(e)
            self.close()
            stopper.set()
        
            

    # close connection
    def close(self):
        self.conn_send2DP.close()
        
        return

    #making analytic decision
    # @setInterval(36000)
    def do(self):
        while True:
            self.conn_listenDBAgent.recv()
            print("Generating P from workload generator")
            result = self.DBclient.query('SELECT * from pending_video')
            clip_list = list(result.get_points(measurement='pending_video'))
            

            if len(clip_list) > 0:
                P_list = self.process_pending(clip_list)
            else:
                print("Can't find any pending videos")

            # send clip_list to Analytics
            self.conn_send2DP.send(P_list)
            print("Keep listening for the next batch of clips from DBA")

    def process_pending(self,clip_list):
        process_num = 1 # needs to less than total number of CPU cores

        P_list = generate_P(P_type=data['DDM']['algo'], clip_list=clip_list)
        print("The length of P:",len(P_list))
        print("[INFO] sending P_decision")

        
        # take away pending videos, so drop it
        self.DBclient.query("DROP MEASUREMENT pending_video")
        return P_list
        




