from multiprocessing.connection import Client,Listener
from util.SetInterval import setInterval
from optimal_downsampling_manager.resource_predictor.table_estimator import get_context,IATable,drop_measurement_if_exist

from influxdb import InfluxDBClient
from pathlib import Path
import datetime
import threading
import time
import os
import csv
import copy
import yaml
import sys

trigger_interval = datetime.timedelta(hours=6) # hours
f_c = 24*60
pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]

ANALY_LIST = ["people_counting","illegal_parking0"]

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

# iATable = IATable(False)
class WorkloadGen():
    def __init__(self):
        self.pending_list = list()
        self.DBclient = InfluxDBClient('localhost', data['global']['database'], 'root', 'root', 'storage')
        # the current time 
        self.cur_clock = datetime.datetime(year = 2020, month = 11, day = 30, hour = 15)   
        #the last updated time
        self.last_updated_clocks = datetime.datetime(year = 2020, month = 11, day = 3, hour = 18)

        self.end_day = 9

        self.cur_day = 9
        self.cur_hour = 0
        self.algo_type = 'FIFO' # FIFO,EF,EFR,Greedy


        self.conn_send2SLE = None
        self.conn_listen2AP = None
        self.ready = threading.Event()
        self.lock=threading.Lock()
        


    def open_SLE_sending_port(self):
        while True:
            time.sleep(1)
            try:
                if self.conn_send2SLE is None:
                    address = ('localhost',int(data['global']['camera2SLE']))
                    self.conn_send2SLE = Client(address)
                    print("[INFO] Connected with SLE...")
            except Exception as e:
                print("Not detecting SLE, reconnecting...")

    ## this port is for simulation
    def open_AP_listening_port(self):
        address = ('localhost', int(data['global']['SLE2camera']))
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


    @setInterval(1)
    def check_ready(self):

        SLE_ready = self.conn_send2SLE 
        AP_ready = self.conn_listen2AP
        if SLE_ready and AP_ready:
            self.ready.set()


    #set port, run monitor    
    def run(self):
        self.ready.wait() #wait every port set up
        print("[INFO] Camera is running the task")
        try:
            self.do()
            input("Press any key to stop...")
            print()
        except Exception as e:    
            print(e)
            
        
    # @setInterval(trigger_interval*3600)
    def do(self):
        while True:
            t = threading.Thread(target=self.SLE_gen_workload)
            t.start()
            t.join()    
            print("Listening from AP & Waiting for generating the next batch of video...")
            finish = self.conn_listen2AP.recv() 
            self.last_updated_clocks = self.cur_clock
            self.cur_clock += trigger_interval
            break

        print("Evaluation finish!!!")
        
            
    def SLE_gen_workload(self):
        try:
            self.lock.acquire()
            print("Generate new pending list...")
            result_list = []
            i = copy.copy(self.last_updated_clocks)
            
            ## if we need time-series
            # while i <= self.cur_clock:
                # result = self.DBclient.query("SELECT * FROM raw_"+ str(i.month) +"_"+str(i.day)) 
                # print("SELECT * FROM raw_"+ str(i.month) +"_"+str(i.day))
                # result_list += list(result.get_points(measurement="raw_"+ str(i.month) +"_"+str(i.day)))
                # i += trigger_interval
            ## if we just want to specify some videos    

            
<<<<<<< HEAD
            table_name = 'sample_11_5'
=======
            table_name = 'sample_11_9'
>>>>>>> a63bce643ede0a669c3e6fe62df7eb4f3ec1e9fe
            result = self.DBclient.query("select * from "+table_name)
            result_list = list(result.get_points(measurement=table_name))


            for r in result_list[100:]:
                v = r['name'].split("/")[-1]
                info_v = v.split("_")
                date = info_v[-2].split("-")
                year = int(date[0])
                month = int(date[1])
                day = int(date[2])
                time = os.path.splitext(info_v[-1])[0].split('-')
                hour = int(time[0])
                video_datetime = datetime.datetime(year,month,day,hour)
                if video_datetime<=self.cur_clock:
                    json_body = [
                        {
                            "measurement": "pending_video",
                            "tags": {
                                "name":r['name'],
                                "a_type":ANALY_LIST[1],
                                "prev_fps":int(24),
                                "prev_bitrate":int(1000),
                                "fps":int(r['fps']),
                                "bitrate":int(r['bitrate']),
                                "a_parameter":int(1)
                            },
                            "fields": {
                                "host": "webcamPole1"
                            }
                        }
                    ]
                    self.DBclient.write_points(json_body)
            
            self.lock.release()
            self.conn_send2SLE.send(True)
            print("Send signal to SLE")
        except Exception as e:
            print(e)

    

    def log_database(self,clip_date,clip_hour):
        
        if clip_hour != self.cur_hour:
            ## check if dir exist
            if not os.path.isdir("./prob2_"+self.algo_type):
                os.system("mkdir prob2_"+self.algo_type)

            ## log information, used space, clip num
            cur_sumsize = sum(f.stat().st_size for f in self.storage_dir.glob('**/*') if f.is_file())
            result = self.DBclient.query("SELECT * FROM videos_in_server")
            result_list = list(result.get_points(measurement='videos_in_server'))

            preserved_ia_in_server = 0
            with open("./prob2_"+self.algo_type+"/clips_"+str(self.cur_day)+"_"+str(self.cur_hour)+".csv", 'w', newline='') as f:
                writer = csv.writer(f)
               
                for r in result_list:
                    writer.writerow([str(r['name']),str(r['fps']),str(r['bitrate'])])
                    day_of_week, time_of_day = get_context(r['name'])
                    for a in range(2):
                        a_id = pre_a_selected.index(r['a_parameter_'+str(a)])
                        preserved_ia_in_server += iATable.get_estimation(day_of_week=day_of_week,time_of_day=time_of_day, a_type=a, a_parameter=a_id)*f_c *(float(r['fps'])/24.0)
            with open("./prob2_"+self.algo_type+"/info_"+str(self.cur_day)+"_"+str(self.cur_hour)+".csv", 'w', newline='') as f:
                writer = csv.writer(f)
                # Information amount, Used space, Clip number
                writer.writerow([preserved_ia_in_server,cur_sumsize,len(result_list)])

            self.cur_day = clip_date
            self.cur_hour = clip_hour
        else: 
            return
