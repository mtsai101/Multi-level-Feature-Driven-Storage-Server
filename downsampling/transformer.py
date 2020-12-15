import numpy as np
import os
import time

from influxdb import InfluxDBClient
from optimal_downsampling_manager.resource_predictor.table_estimator import get_context


class Transformer:
    def __init__(self):
        self.DBclient = InfluxDBClient('localhost', data['global']['database'], 'root', 'root', 'storage')
        self.parameter = 0
        self.ratio = 1
        self.execute_time = 0
        self.fps = 0 
        self.bitrate = 0

        
    def transform(self,P_decision):
        print("FPS:",P_decision.fps,"Bitrate:",P_decision.bitrate)
        self.execute_time = 0
        try:
            if P_decision.fps > 0  and P_decision.bitrate >0:
                start_time = time.time()

                # file_path = self.rewrite_path(clip_name,self.fps,self.bitrate)

                ## because ffmpeg can't write inplace, so we write to _converting
                tmp_name = os.path.splitext(P_decision.clip_name)[0]+"_converting.mp4"
                cmd = "mv %s %s"%(P_decision.clip_name, tmp_name)
                os.system(cmd)

                cmd = 'ffmpeg -hwaccel cuvid -c:v hevc_cuvid -i %s -c:v hevc_nvenc -rc cbr_hq -r %d -b:v %dK -maxrate:v %dK -y %s' %(tmp_name, P_decision.fps, P_decision.bitrate, P_decision.bitrate, P_decision.clip_name)
                os.system(cmd)
                
                self.execute_time = time.time() - start_time

                cmd = "rm %s"%(tmp_name)
                os.system(cmd)
                

                self.save(P_decision)
                # size = os.path.getsize(clip_name)
                # self.ratio = os.path.getsize(file_path) / size

            elif P_decision.fps==-1 and P_decision.bitrate==-1: ## remove the clip from the server
                cmd = "rm %s"%(P_decision.clip_name)
                os.system(cmd)
                self.DBclient.query("DELETE FROM videos_in_server WHERE \"name\"=\'"+P_decision.clip_name+"\'")
            else:
                print("[ERROR] Error downsampling type")
        except Exception as e:
            print(e)

        

    def save(self,P_decision):
        old_row = self.DBclient.query("SELECT * FROM videos_in_server WHERE \"name\"=\'"+P_decision.clip_name+"\'")
        old_row = list(old_row.get_points(measurement="videos_in_server"))[0]
        self.DBclient.query("DELETE FROM videos_in_server WHERE \"name\"=\'"+P_decision.clip_name+"\'")
        
        json_body = [
            {
                "measurement": "videos_in_server",
                "tags": {
                    "name": str(P_decision.clip_name),
                    
                },
                "time":old_row['time'],
                "fields": {
                    "fps":float(P_decision.fps),
                    "bitrate":float(P_decision.bitrate),
                    "a_para_illegal_parking": float(P_decision.others[0]),
                    "a_para_people_counting": float(P_decision.others[1]),
                    "raw_size":float(P_decision.others[2])
                }
            }
        ]
        
        self.DBclient.write_points(json_body)

        self.fps = 0
        self.bitrate = 0
        self.ratio = 1

    # def rewrite_path(self,clip_name,fps,bitrate):
    #     name = clip_name.split('/')[-1]
    #     new_dir = os.path.join('./dataSet/down_result_videos',str(fps)+"_"+str(bitrate))
    #     if not os.path.isdir(new_dir):
    #         os.mkdir(new_dir)
    #     file_path = os.path.join(new_dir,name)

    #     return file_path
    