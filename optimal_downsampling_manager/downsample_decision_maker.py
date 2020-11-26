from multiprocessing.connection import Client,Listener
from .decision_type import Decision
from .P_generator import generate_P
from .divide_week import weeklist
from influxdb import InfluxDBClient
from util.SetInterval import setInterval
import numpy as np
import threading
import random
import time
import math



class DownSampleDecisionMaker(object):
    def __init__(self):
        self.conn_send2DP = None
        self.conn_listenVirtualCamera = None

        self.ready = threading.Event()
        
        self.DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'video_edge')
        self.VClistener = Listener(('localhost',3000))


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
                
    def open_DP_sending_port(self):
        while True:
            time.sleep(1)
            try:
                if self.conn_send2DP is None:
                    address = ('localhost',7001)
                    self.conn_send2DP = Client(address)
                    print("Connected with Down Platform")
            except Exception as e:
                print("Not detecting Down Platform, reconnecting...")
    
    @setInterval(1)
    def check_ready(self):
        transformer_ready = self.conn_send2DP
        camera_ready = self.conn_listenVirtualCamera
        if transformer_ready and camera_ready:
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
        #self.conn2DP.close()
        
        return

    #making analytic decision
    # @setInterval(36000)
    def do(self):
        while True:
            invoke_time = self.conn_listenVirtualCamera.recv()
            print("Generating P from workload generator")
            result = self.DBclient.query('SELECT * from videos_in_server')
            clip_list = list(result.get_points(measurement='videos_in_server'))
            

            if len(clip_list) > 0:
                t = threading.Thread(target=self.process_pending,args=(clip_list,invoke_time))
                t.start()
                t.join()
            else:
                print("Can't find any pending videos")

            print("Keep listening for the next batch of clips from VC")

    def process_pending(self,clip_list,invoke_time):
        process_num = 1 # needs to less than total number of CPU cores

        P_list = generate_P(P_type='greedy', clip_list=clip_list)
        print("The length of P:",len(P_list))
        print("[INFO] sending P_decision")

        # send clip_list to Analytics
        self.conn_send2DP.send([P_list,invoke_time])




