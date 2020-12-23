import numpy as np
import os
import time

from influxdb import InfluxDBClient
from optimal_downsampling_manager.resource_predictor.table_estimator import get_context
import yaml
import sys
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

os.environ["CUDA_VISIBLE_DEVICES"]="3"
class Transformer:
    def __init__(self):

        self.DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database'], 'root', 'root', 'storage')
        self.total_downsample_time = 0 

    def transform(self,P_decision):
        print("FPS:",P_decision.fps,"Bitrate:",P_decision.bitrate)
        
        try:
            if int(P_decision.fps) > 0  and int(P_decision.bitrate) > 0:
                start_time = time.time()

                # file_path = self.rewrite_path(clip_name,self.fps,self.bitrate)


                ## because ffmpeg can't write inplace, so we write to _converting
                
                # tmp_name = os.path.splitext(P_decision.clip_name)[0]+"_converting.mp4"
                # cmd = "mv %s %s"%(P_decision.clip_name, tmp_name)               
                # os.system(cmd)


                ## convert from prevfps_prevbitrate to fps_bitrate
                parsed_video_path = P_decision.clip_name.split("/")
                converted_folder = str(P_decision.fps)+"-"+str(P_decision.bitrate)
                dest_folder = os.path.join("./storage_server_volume", "converted_videos", converted_folder, parsed_video_path[-2])
                if not os.path.isdir(dest_folder):
                    os.makedirs(dest_folder)
                    print("create", dest_folder)

                
                converted_path = os.path.join(dest_folder, parsed_video_path[-1])
                

                cmd = 'ffmpeg -hwaccel cuvid -c:v hevc_cuvid -i %s -c:v hevc_nvenc -rc cbr_hq -r %d -b:v %dK -maxrate:v %dK -y %s' %(P_decision.clip_name, P_decision.fps, P_decision.bitrate, P_decision.bitrate, converted_path)
                # print(cmd)
                os.system(cmd)
                
                execution_time = time.time() - start_time
                self.total_downsample_time += execution_time
                # cmd = "rm %s"%(tmp_name)
                # print(cmd)
                # os.system(cmd)
                

                ratio = os.path.getsize(converted_path) / (P_decision.others[2]*pow(2,20))

                self.save_converted_video(P_decision, ratio, execution_time)

                # self.save(P_decision, converted_folder)

            # elif P_decision.fps==-1 and P_decision.bitrate==-1: ## remove the clip from the server
            #     cmd = "rm %s"%(P_decision.clip_name)
            #     # os.system(cmd)
            #     self.DBclient.query("DELETE FROM videos_in_server WHERE \"name\"=\'"+P_decision.clip_name+"\'")
            else:
                print("[ERROR] Error downsampling type")
        except Exception as e:
            print(e)
            sys.exit()

        
    def save_converted_video(self, P_decision, ratio, execution_time):
        json_body = [
            {
                "measurement": "down_result",
                "tags": {
                    "month": P_decision.month,
                    "day": P_decision.day,
                    "day_idx": P_decision.day_idx,
                    "time_idx": P_decision.time_idx,
                    "prev_fps": P_decision.prev_fps,
                    "prev_bitrate": P_decision.prev_bitrate,
                    "fps": P_decision.fps,
                    "bitrate": P_decision.bitrate
                },
                "fields": {
                    "name": str(P_decision.clip_name),
                    "execution_time": float(execution_time),
                    "ratio": float(ratio),
                    "raw_size":float(P_decision.others[2])
                }
            }
        ]
        
        self.DBclient.write_points(json_body)


    def update_stored_video(self,P_decision):
        old_row = self.DBclient.query("SELECT * FROM videos_in_server WHERE \"name\"=\'"+P_decision.clip_name+"\'")
        old_row = list(old_row.get_points(measurement="videos_in_server"))[0]
        
        json_body = [
            {
                "measurement": "videos_in_server",
                "tags": {
                    "name": str(P_decision.clip_name)
                    
                },
                "time":old_row['time'],
                "fields": {
                    "fps":float(P_decision.fps),
                    "bitrate":float(P_decision.bitrate),
                    "prev_fps":float(P_decision.prev_fps),
                    "prev_bitrate":float(P_decision.prev_bitrate),
                    "a_para_illegal_parking": float(P_decision.others[0]),
                    "a_para_people_counting": float(P_decision.others[1]),
                    "raw_size":float(P_decision.others[2])
                }
            }
        ]
        
        self.DBclient.write_points(json_body)

        self.ratio = 1


    # def rewrite_path(self,clip_name,fps,bitrate):
    #     name = clip_name.split('/')[-1]
    #     new_dir = os.path.join('./dataSet/down_result_videos',str(fps)+"_"+str(bitrate))
    #     if not os.path.isdir(new_dir):
    #         os.mkdir(new_dir)
    #     file_path = os.path.join(new_dir,name)

    #     return file_path
    