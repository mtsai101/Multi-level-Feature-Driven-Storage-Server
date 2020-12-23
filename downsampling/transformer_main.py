#encoding=utf-8
from multiprocessing.connection import Client,Listener
from multiprocessing import Process
from multiprocessing import Pool
from .transformer import Transformer
from optimal_downsampling_manager.decision_type import Decision
import multiprocessing
import pickle
import time
import os
import csv
import yaml

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

class DownSample_Platform():
    def __init__(self):
        address = ('localhost', int(data['global']['DDM2DP']))     
        self.listener = Listener(address)
        self.conn_send2DBA = None
        
        ## this port is for simulator

    ## this port is for simulator
    def open_DBA_sending_port(self):
        print("[INFO] Listening from DB Agent")
        while True:
            time.sleep(3)
            try:
                if self.conn_send2DBA is None:
                    address = ('localhost',int(data['global']['DP2agent']))
                    self.conn_send2DBA = Client(address)
                    print("Connected with DB Agent")
            except Exception as e:
                print("No DB Agent, reconnecting...")
                
    def run(self):
        while True:
            try:
                print("[INFO] Listening from DDM")
                conn = self.listener.accept()
                print('connection accepted from', self.listener.last_accepted)
                while True:
                    P_decision_list = conn.recv()
                    try:
                        print("T_decision length: ",len(P_decision_list))
                        
                        total_downsample_time = self.hire_transformer(P_decision_list)
                        
                        # log downsampling time by timestamp...
                        # self.log_downsample_time(invoke_time,total_downsample_time)

                        # tell VC can load the next batch of clips 
                        if self.conn_send2DBA is not None:
                            print("Signal to Virtual Camera")
                            self.conn_send2DBA.send(True)
                        
                    except Exception as e:
                        print(e)
            except Exception as e:
                # pickle a people_dict to a file
                print("[WARN] Keep listening...")
            finally:
                conn.close()
                time.sleep(1)

            

    def terminate_listen(self):
        self.listener.close()

    def hire_transformer(self, P_decision_list):
        total_downsample_time = 0
        for P_decision in P_decision_list:
            try:
                transformer = Transformer()
                
                print('[INFO] "fps:{}, bitrate:{}"...'.format(P_decision.fps,P_decision.bitrate))
                print('[INFO]',multiprocessing.current_process())
                
                transformer.transform(P_decision)
                
            except Exception as e:
                print(e)
        
        print("[INFO] Downsamping end up...")
        return total_downsample_time


    def log_downsample_time(self,invoke_time,total_downsample_time):
        algo_type='greedy'
        path = "./prob2_"+algo_type
        day = invoke_time[0]
        time = invoke_time[1]
        if not os.path.isdir(path):
            os.mkdir(path) 
        
        with open(os.path.join(path,"down_time_"+str(day)+"_"+str(int(time))+".csv"),'a',newline='') as f:
            writer = csv.writer(f)
            writer.writerow([total_downsample_time])

        


