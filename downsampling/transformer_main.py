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

class DownSample_Platform():
    def __init__(self):
        address = ('localhost', 7001)     
        self.listener = Listener(address)
        self.conn_send2VC = None
        
        ## this port is for simulator

    ## this port is for simulator
    def open_VC_sending_port(self):
        print("[INFO] Listening from Virtual Camera")
        while True:
            time.sleep(3)
            try:
                if self.conn_send2VC is None:
                    address = ('localhost',6001)
                    self.conn_send2VC = Client(address)
                    print("Connected with Virtual Camera")
            except Exception as e:
                print("No Virtual Camera, reconnecting...")
                
    def run(self):
        while True:
            try:
                print("[INFO] Listening port DDM")
                conn = self.listener.accept()
                print('connection accepted from', self.listener.last_accepted)
                while True:
                    P_decision_list,invoke_time = conn.recv()
                    try:
                        print("T_decision length: ",len(P_decision_list))
                        
                        total_downsample_time = self.hire_transformer(P_decision_list)
                        
                        # log downsampling time by timestamp...
                        self.log_downsample_time(invoke_time,total_downsample_time)

                        # tell VC can load the next batch of clips 
                        if self.conn_send2VC is not None:
                            print("Signal to Virtual Camera")
                            self.conn_send2VC.send(True)
                        
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
                total_downsample_time += transformer.execute_time
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

        


