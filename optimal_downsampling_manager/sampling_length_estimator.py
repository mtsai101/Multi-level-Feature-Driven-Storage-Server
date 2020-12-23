from multiprocessing.connection import Client,Listener
from .decision_type import Decision
from .L_generator import generate_L
from influxdb import InfluxDBClient
from virtual_camera import WorkloadGen
from util.SetInterval import setInterval
import datetime
import threading
import numpy as np
import time
import random
import os
import yaml

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)


trigger_Interval = 6
class SamplingLengthEstimator(object):
    def __init__(self):
        self.conn_listenVirtualCamera = None      
        self.conn_send2Analytic = None
        self.ready = threading.Event()
    
        self.DBclient = InfluxDBClient('localhost', data['global']['database'], 'root', 'root', 'storage')
        self.VClistener = Listener(('localhost',int(data['global']['camera2SLE'])))
    

    def open_VC_listening_port(self):
        while True:
            time.sleep(3)
            try:
                if self.conn_listenVirtualCamera is None:
                    print("[INFO] Listening from Virtual Camera")
                    self.conn_listenVirtualCamera = self.VClistener.accept()
                    print('connection accepted from', self.VClistener.last_accepted)

            except Exception as e:
                print("[INFO] Close listening port")
                self.conn_listenVirtualCamera.close()  
                self.conn_listenVirtualCamera = None  
                print(e)

   
                
    def open_AP_sending_port(self):
        while True:
            time.sleep(3)
            try:
                if self.conn_send2Analytic is None:
                    address = ('localhost',int(data['global']['SLE2AP']))
                    self.conn_send2Analytic = Client(address)
                    print("Connected with Analy Platform")
            except Exception as e:
                print("Not detecting Analy Platform, reconnecting...")
    
    @setInterval(1)
    def check_ready(self):
        #print("[DEBUG] Checking state...")

        analyst_ready = self.conn_send2Analytic 
        camera_ready = self.conn_listenVirtualCamera
        if analyst_ready and camera_ready:
            self.ready.set()

    
    #set port, run monitor    
    def run(self):
        self.ready.wait() #wait every port set up
        
        try:
            print("[INFO] SLE is running the task")
            stopper = self.do()

        except Exception as e:    
            print(e)
            self.close()
            stopper.set()
        
            

    # close connection
    def close(self):
        # self.conn2AnalyPlatform.close()

        return

    #making analytic decision
    # @setInterval(trigger_Interval*3600)
    def do(self):
        while True:
            pending_video_ready = self.conn_listenVirtualCamera.recv()
            print("Generating L from workload generator")
            result = self.DBclient.query('SELECT * from pending_video')
            clip_list = list(result.get_points(measurement='pending_video'))
            if len(clip_list) > 0:
                self.process_pending(clip_list)
            else:
                print("Can't find any pending videos")

            print("Keep listening for the next batch of clips from VC")
            
    def process_pending(self,clip_list):
        process_num = 1 # needs to less than total number of CPU cores

        L_list = generate_L(L_type=data['SLE']['algo'], clip_list=clip_list, process_num=process_num)
        print("L list length: ",len(L_list))
        print("[INFO] sending L_decision")
        
        # send clip_list to Analytics
        self.conn_send2Analytic.send(L_list)

        # take away pending videos, so drop it
        self.DBclient.query("DROP MEASUREMENT pending_video")


