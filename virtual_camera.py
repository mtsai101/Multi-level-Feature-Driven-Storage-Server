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

trigger_interval = datetime.timedelta(hours=6) # hours
f_c = 24*60
pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]
iATable = IATable(False)
class WorkloadGen():
    def __init__(self):
        self.mode = 1
        self.pending_list = list()
        self.DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
        # the current time 
        self.cur_clock = datetime.datetime(year = 2020, month = 11, day = 4, hour = 6)   
        #the last updated time
        self.last_updated_clocks = datetime.datetime(year = 2020, month = 11, day = 3, hour = 18)

        self.end_day = 9

        self.cur_day = 9
        self.cur_hour = 0
        self.algo_type = 'FIFO' # FIFO,EF,EFR,Greedy


        self.conn_send2SLE = None
        self.conn_listen2AP = None
        self.conn_send2DDM = None
        self.conn_listen2DP = None
        self.ready = threading.Event()
        self.lock=threading.Lock()
        
    
        if self.mode == 2:
            self.DDM_size_threshold = 5*1024
            self.DDMflag = 0
            self.DDMlist = []
            self.DDMDir = Path("./storage_space")

            for i in range(9,14):
                result = self.DBclient.query("SELECT * FROM raw_11_"+str(i))
                self.DDMlist.extend(list(result.get_points(measurement="raw_11_"+str(i))))

            try:
                os.system("rm -rf ./ssd/space_experiment/raw*")
                print("Clean the video")
            except:
                pass
            try:
                os.system("rm -rf ./prob2_"+self.algo_type)
                print("Delete csv")
            except:
                pass
            try:
                self.DBclient.query("DROP MEASUREMENT videos_in_server")
                print("Delete the videos_in_server table")
            except:
                pass



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


    def open_SLE_sending_port(self):
        while True:
            time.sleep(1)
            try:
                if self.conn_send2SLE is None:
                    address = ('localhost',5000)
                    self.conn_send2SLE = Client(address)
                    print("[INFO] Connected with SLE...")
            except Exception as e:
                print("Not detecting SLE, reconnecting...")

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

    @setInterval(1)
    def check_ready(self):
        if self.mode == 1:
            SLE_ready = self.conn_send2SLE 
            AP_ready = self.conn_listen2AP
            if SLE_ready and AP_ready:
                self.ready.set()
        elif self.mode == 2:
            DDM_ready = self.conn_send2DDM
            DP_ready = self.conn_listen2DP
            if DDM_ready and DP_ready:
                self.ready.set()
        else:
            print("Error camera mode!")

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
            if self.mode == 1:
                t = threading.Thread(target=self.SLE_gen_workload)
                t.start()
                t.join()    
                print("Listening from AP & Waiting for generating the next batch of video...")
                finish = self.conn_listen2AP.recv() 
                self.last_updated_clocks = self.cur_clock
                self.cur_clock += trigger_interval
                break
                

            elif self.mode ==2:
                t = threading.Thread(target=self.DDM_gen_workload)
                t.start()
                t.join()
                print("Listening from DP & Waiting for generating the next batch of video...")
                finish = self.conn_listen2DP.recv()

                if self.DDMflag ==len(self.DDMlist)-1:
                    break
            

        print("Evaluation finish!!!")
        
            
    def SLE_gen_workload(self):
        try:
            self.lock.acquire()
            print("Generate new pending list...")
            result_list = []
            i = copy.copy(self.last_updated_clocks)

            while i <= self.cur_clock:
                result = self.DBclient.query("SELECT * FROM raw_"+ str(i.month) +"_"+str(i.day))            
                result_list += list(result.get_points(measurement="raw_"+ str(i.month) +"_"+str(i.day)))
                i += datetime.timedelta(days=1)

            for r in result_list:
                v = r['name'].split("/")[-1]
                info_v = v.split("_")
                date = info_v[2].split("-")
                year = int(date[0])
                month = int(date[1])
                day = int(date[2])
                time = os.path.splitext(info_v[3])[0].split(":")
                hour = int(time[0])
                video_datetime = datetime.datetime(year,month,day,hour)
              
                if video_datetime<=self.cur_clock:
                    json_body = [
                        {
                            "measurement": "pending_video",
                            "tags": {
                                "name":r['name'],
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

    def DDM_gen_workload(self):
        try:
            self.lock.acquire()
            print("Generate new pending list...")
            accumulate_size = 0
            for r in self.DDMlist[self.DDMflag:]:
                clip_date = int(r['name'].split("/")[-2].split("_")[-1])
                clip_hour = int(r['name'].split("/")[-1].split("_")[-1].split(":")[0])

                if self.DDMflag == len(self.DDMlist)-1:
                    print("Already look all video!!!")
                    self.lock.release()
                    return
                else:
                    self.DDMflag +=1
                

                # log every hour
                # self.log_database(clip_date, clip_hour)


                dir_clip = r['name'].split("/")
                new_path = os.path.join("./ssd/space_experiment",dir_clip[-2])
                if not os.path.isdir(new_path):
                    os.mkdir(new_path) 
                clip_name = os.path.join(new_path,dir_clip[-1])
                cmd = "cp %s %s"%(r['name'], clip_name)
                print(cmd)
                os.system(cmd)
                result = self.DBclient.query("SELECT * FROM analy_result_greedy WHERE \"name\"=\'"+r['name']+"\'")
                result_list = list(result.get_points(measurement="analy_result_greedy"))

                a_parameter_ill = 5.0
                a_parameter_peo = 5.0

                for r in result_list:
                    if r['a_type']=='illegal_parking0':
                        a_parameter_ill = r['a_parameter']
                    elif r['a_type']=='people_counting':
                        a_parameter_peo = r['a_parameter']

                    

                raw_size = os.path.getsize(r['name']) / pow(2,20)
                if raw_size<=0:
                    raw_size = 6
                json_body = [
                    {
                        "measurement": "videos_in_server",
                        "tags": {
                            "name": str(clip_name),
                            "fps":float(24.0),
                            "bitrate":float(1000.0),
                            "host": "webcamPole1"
                        },
                        "fields": {
                            "a_parameter_0": float(a_parameter_ill),
                            "a_parameter_1": float(a_parameter_peo),
                            "raw_size":float(raw_size)
                        }
                    }
                ]
                self.DBclient.write_points(json_body)
                

                sumsize = sum(f.stat().st_size for f in self.DDMDir.glob('**/*') if f.is_file())
                sumsize = sumsize/pow(2,20)
                if sumsize > self.DDM_size_threshold:
                    print(sumsize,self.DDM_size_threshold)
                    print("Out of size at %s, trigger prob2..."%(r['name']))
                    break
            
            self.lock.release()
            self.conn_send2DDM.send([clip_date,clip_hour])
            print("Send signal to DDM")
        except Exception as e:
            print(e)

    def log_database(self,clip_date,clip_hour):
        
        if clip_hour != self.cur_hour:
            ## check if dir exist
            if not os.path.isdir("./prob2_"+self.algo_type):
                os.system("mkdir prob2_"+self.algo_type)

            ## log information, used space, clip num
            cur_sumsize = sum(f.stat().st_size for f in self.DDMDir.glob('**/*') if f.is_file())
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
