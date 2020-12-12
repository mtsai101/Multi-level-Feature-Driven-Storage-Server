#encoding=utf-8
from optimal_downsampling_manager.decision_type import Decision
from multiprocessing.connection import Client,Listener
from multiprocessing import Process,Pool,managers,Manager
from optimal_downsampling_manager.resource_predictor.table_estimator import IATable,AnalyTimeTable
from .analyst import Analyst
from .darknet import darknet
import multiprocessing
import threading
import pickle
import time
import csv
import re
import os
import queue
import sys
import yaml

configPath = "./analytics/darknet/cfg/yolov3.cfg"
weightPath = "./analytics/darknet/yolov3.weights"
metaPath = "./analytics/darknet/cfg/coco.data"
# #2048*1536
# park_poly1 = [[750, 532], [594, 680], [1440, 833], [1457, 639], [1068, 536], [804, 532]]
# park_poly2 = [[1506, 651], [1684, 829], [2022, 763], [1845, 585], [1539, 614]]
# park_poly3 = [[222, 614], [589, 556], [689, 1448], [20, 1461], [16, 1068], [201, 660]]
#416*416
park_poly1 = [[42,168],[121 ,150],[140 ,391],[ 2, 397],[0, 280]]
park_poly2 = [[118, 188],[292, 220],[293, 172],[156, 143]]
park_poly3 = [[305, 178],[342 ,224],[411, 204],[385, 166]]

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

class Analytic_Platform():
    def __init__(self):
        address = ('localhost', data['global']['SLE2AP'])     
        self.listener = Listener(address)
        self.conn_send2VC = None
        self.netMain = None
        self.metaMain = None
        self.altNames = None
        self.analyst = None

    ## this port is for simulator
    def open_VC_sending_port(self):
        print("[INFO] Listening from Virtual Camera")
        while True:
            time.sleep(3)
            try:
                if self.conn_send2VC is None:
                    address = ('localhost',int(data['global']['SLE2camera']))
                    self.conn_send2VC = Client(address)
                    print("Connected with Virtual Camera")
            except Exception as e:
                print("No Virtual Camera, reconnecting...")

    def run(self):
        # init the net and analyst, only do them once
        self.init_metadata()
        self.init_analyst()

        while True:
            try:
                print("[INFO] Listening from SLE")
                conn = self.listener.accept()
                print('connection accepted from', self.listener.last_accepted)
                while True:
                    print("[INFO]Waiting msg...")
                    L_decision_list = conn.recv()
                    
                    try:
                        print("Total L_decision: ",str(len(L_decision_list)))
                        
                        self.hire_analyst(L_decision_list)

                        # tell VC can load the next batch of clips 
                        if self.conn_send2VC is not None:
                            print("Signal to Virtual Camera")
                            self.conn_send2VC.send(True)

                    except Exception as e:
                        print("Exception here",e)
                        
            except Exception as e:
                # pickle a people_dict to a file
                print("[WARN] Keep listening...")
            finally:
                conn.close()


    def terminate_listen(self):
        self.listener.close()





    def init_metadata(self):

        if not os.path.exists(configPath):
            raise ValueError("Invalid config path `" +
                            os.path.abspath(configPath)+"`")
        if not os.path.exists(weightPath):
            raise ValueError("Invalid weight path `" +
                                os.path.abspath(weightPath)+"`")
        if not os.path.exists(metaPath):
            raise ValueError("Invalid data file path `" +
                                os.path.abspath(metaPath)+"`")

        if self.netMain is None:
            self.netMain = darknet.load_net_custom(configPath.encode(
                "ascii"), weightPath.encode("ascii"), 0, 1)  # batch size = 1

        if self.metaMain is None:
            self.metaMain = darknet.load_meta(metaPath.encode("ascii"))
        if self.altNames is None:
            try:
                with open(metaPath) as metaFH:
                    metaContents = metaFH.read()
                    import re
                    match = re.search("names *= *(.*)$", metaContents,
                                        re.IGNORECASE | re.MULTILINE)
                    if match:
                        result = match.group(1)
                    else:
                        result = None
                    try:
                        if os.path.exists(result):
                            with open(result) as namesFH:
                                namesList = namesFH.read().strip().split("\n")
                                self.altNames = [x.strip() for x in namesList]
                    except TypeError as e:
                        print(e)
            except Exception as e:
                print(e)
        


    def init_analyst(self):
        global park_poly1,park_poly2,park_poly3

        print("init data")
        self.analyst = Analyst() 
        self.analyst.save_result_video(False) ##save the result video or not 
        self.analyst.set_illegal_region(park_poly1)
        self.analyst.set_illegal_region(park_poly2)
        self.analyst.set_illegal_region(park_poly3)
        self.analyst.set_net(self.netMain, self.metaMain, self.altNames)

        

    def hire_analyst(self, L_decision_list):
        try:
            for L_decision in L_decision_list:
                self.analyst.set_video_clip(L_decision)
                print('[INFO]',multiprocessing.current_process())
                print('[INFO] a_type: {}, a_param: {}, fps: {}, bitrate:{}'
                            .format(L_decision.a_type, 
                                    L_decision.a_param, 
                                    L_decision.fps,
                                    L_decision.bitrate))

                self.analyst.analyze(
                                L_decision.clip_name,
                                L_decision.a_type, # analy type
                                L_decision.a_param,
                            )
                self.analyst.analyze_save(L_decision)
                self.analyst.analyze_save_per_frame(L_decision)
                self.analyst.clean()     
                                
        except Exception as e:
            print(e)
        

        print("[INFO] Analyzing end up...")
        print("[INFO] Updating the prediction model now...")
        # update the prediction table
        # updateIA = threading.Thread(target=IATable,args=(True,))
        # updateIA.start()
        # updateAnalyTime = threading.Thread(target=AnalyTimeTable,args=(True,))
        # updateAnalyTime.start()

        # updateIA.join()
        # updateAnalyTime.join()



