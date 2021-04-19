from influxdb import InfluxDBClient
from optimal_downsampling_manager.resource_predictor.table_estimator import AnalyTimeTable, DownTimeTable, DownRatioTable, IATable, drop_measurement_if_exist
import os
import csv
import ast
import yaml

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

DBclient = InfluxDBClient(host=data['global']['database_ip'], port=data['global']['database_port'], database=data['global']['database_name'], username='root', password='root')

if __name__=='__main__':
    
    # init raw video database
    # for i in range(10,16):
    #     db_name = "raw_11_"+str(i)
    #     if i<10:
    #         i_ = "0"+str(i)
    #     else:
    #         i_=str(i)

    #     base_dir = "./storage_server_volume/SmartPole/Pole1/2020-11-"+i_+"_00-00-00"
    #     video_li = os.listdir(base_dir)
    #     video_li = sorted(video_li, key= lambda x: x)
    #     for v in video_li:
    #         v_path = os.path.join(base_dir,v)
            
    #         if os.path.isdir(v_path):
    #             continue
    #         json_body = [
    #                             {
    #                                 "measurement": db_name,
    #                                 "tags": {
    #                                     "name": str(v_path)
                                        
    #                                 },
    #                                 "fields": {
    #                                     "host": "webcamPole1"
    #                                 }
    #                             }
    #                         ]
            
    #         DBclient.write_points(json_body)

    # init sample video database

    # base_dir = "./storage_server_volume/converted_videos/"
    # quality_dir = os.listdir(base_dir)
    # for q in quality_dir:
    #     fps, bitrate = q.split('-')
    #     quality_li = os.path.join(base_dir,q)
    #     video_dir = os.listdir(quality_li)
    #     for video_date in video_dir:
    #         video_li = os.path.join(quality_li, video_date)
    #         video_internal_li = os.listdir(video_li)
    #         video_internal_li = sorted(video_internal_li, key= lambda x: x)
    #         for v in video_internal_li:
    #             v_path = os.path.join(video_li,v)

    #             if os.path.isdir(v_path):
    #                 continue
    #             date =  v_path.split('/')[-2].split('_')[0].split('-')
                
    #             # print("sample_11_"+str(int(date[2])), v_path, fps, bitrate)
    #             json_body = [
    #                                 {
    #                                     "measurement": "sample_11_"+str(int(date[2])),
    #                                     "tags": {
    #                                         "name": str(v_path),
    #                                         "fps": int(fps),
    #                                         "bitrate": int(bitrate)                                            
    #                                     },
    #                                     "fields": {
    #                                         "host": "webcamPole1"
    #                                     }
    #                                 }
    #                             ]
    #             DBclient.write_points(json_body)

    # init analy result
    # json_body = [
    #     {
    #         "measurement": "analy_result",
    #         "tags": {
    #             "a_type": "people_counting",
    #             "name": "./storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-09_09:31:53.mp4",
    #             "host": "webcamPole1"
    #         },
    #         "fields": {
    #             "day_of_week":int(0),
    #             "time_of_day":int(4),
    #             "a_parameter": float(100.0),
    #             "fps": float(24.0),
    #             "bitrate": float(1000.0),
    #             "time_consumption": float(0.0),
    #             "target": float(0.0)
    #         }
    #     }
    # ]
    # DBclient.write_points(json_body)
    
    # # # init down_result
    # json_body = [
    #         {
    #             "measurement": "down_result",
    #             "tags": {
    #                 "name": "./storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-09_09:31:53.mp4",
    #                 "host": "webcamPole1",
    #             },
    #             "fields": {
    #                 "day_of_week":int(0),
    #                 "time_of_day":int(4),
    #                 "a_parameter": float(100.0),
    #                 "fps": float(24.0),
    #                 "bitrate": float(1000.0),
    #                 "time_consumption": float(0.0),
    #                 "origin_size":float(0.0),
    #                 "result_size":float(0.0),
    #                 "ratio":float(1.0)
    #             }
    #         }
    #     ]
    # DBclient.write_points(json_body)

    # iaTable = IATable(True)
    # analyTimeTable = AnalyTimeTable(True)
    # downRatioTable = DownRatioTable(True)
    # downTimeTable = DownTimeTable(True)


    ## mv day-00 to correct
    # base_dir = "/mnt/nas/fog/SmartPoleVideo/SaveEveryHour/shot_detection"
    # li = os.listdir(base_dir)

    # for l in li:
    #     video_dir = os.path.join(base_dir, l)
    #     video_li = os.listdir(video_dir)

    #     day = int(l.split('_')[0].split('-')[-1])
    #     for v in video_li:
    #         video_path = os.path.join(video_dir,v)
    #         if os.path.isfile(video_path):
    #             if day != int(v.split('_')[1].split('-')[-1]):
    #                 cmd = "mv " + video_path +" .."  
    #                 # os.system(cmd)
    #                 print(cmd)
    #                 back_video = "background_"+v
    #                 cmd = "mv " + os.path.join(video_dir,"background", back_video) + " ../.."
    #                 print(cmd)
    #                 # os.system(cmd)

    # # Init the shot list to databases
    # shot_list=[]
    # with open('./shot_list_4_29.csv', 'r') as csvfile:
    #     rows = csv.reader(csvfile)
    #     for row_s in rows:
    #         vid_date = row_s[-2]
    #         vid_name = row_s[0]
    #         vid_shot_list = row_s[1]
    #         shot_list.append((vid_date, vid_name, vid_shot_list))
            

    # sorted_shot_list = sorted(shot_list, key= lambda x: x[1])
    # for s in sorted_shot_list:
    #     json_body = [
    #                 {
    #                     "measurement": "shot_list",
    #                     "tags": {
    #                         "base_path": data['global']['base_path'],
    #                         "storage_path": data['global']['storage_path'],
    #                         "date": str(s[0]),
    #                         "name": str(s[1])
    #                     },
    #                     "fields": {
    #                         "list": str(s[2])
    #                     }
    #                 }
    #             ]
    #     DBclient.write_points(json_body)

    # Init the shot list to databases
    # vis_entropy_list = []
    # with open('./visual_features_entropy_unnormalized_4_15.csv', 'r') as csvfile:
    #     rows = csv.reader(csvfile)
    #     next(rows)
    #     for r in rows:
    #         name = r[0].split("/")[-1]
    #         color = float(r[1]); edge = float(r[2])
    #         conv = float(r[3]);  temp = float(r[4])
    #         vis_entropy_list.append((name, color, edge, conv, temp))
            

    # sorted_vis_entropy_list = sorted(vis_entropy_list, key= lambda x: x[0])
    # for s in sorted_vis_entropy_list:
    #     json_body = [
    #                 {
    #                     "measurement": "visual_features_entropy_unnormalized_arena",
    #                     "tags": {
    #                         "name": s[0],
    #                         "base_path": data['global']['base_path'],
    #                         "storage_path": data['global']['storage_path']
    #                     },
    #                     "fields": {
    #                         "color": s[1],
    #                         "edge": s[2],
    #                         "conv": s[3],
    #                         "temp": s[4]
    #                     }
    #                 }
    #             ]
    #     DBclient.write_points(json_body, time_precision='ms')


    ## Pressure test...
    # json_body = []
    # import time 
    # day = 0
    # hour = 0
    # frame_id = 0
    # for f in range(850*24):

    #     ## save info_amount by type
    #     json_body.append(
    #         {
    #             "measurement": "insertTest",
    #             "tags": {
    #                 "name": "path_"+str(day)+str(hour),
    #                 "hour": int(hour),
    #                 "idx": int(frame_id)
    #             },
    #             "fields": {
                    
    #                 "value":str(f)+"hello"
    #             }
    #         }
    #     )
    #     frame_id += 1
    #     if frame_id==850:
    #         hour = hour+1
    #         frame_id = 0
    #         if hour==24:
    #             hour = 0
    #             day +=1


    # s = time.time()
    # DBclient.write_points(json_body, database='storage', time_precision='ms', batch_size=80000, protocol='json')
    # print("%.2f"%(time.time()-s))

    # result = DBclient.query("SELECT * FROM analy_result_raw_per_frame_inshot_4_9")
    # result_list = list(result.get_points(measurement='analy_result_raw_per_frame_inshot_4_9'))

    # back_DBclient = InfluxDBClient(host=data['global']['database_ip'], port=data['global']['port'], database=data['global']['database_name'], username='root', password='root')
    # json_body=[] 
    # for f in result_list:
        
    #     if f['name'].split('/')[-2]=="2020-11-04_00-00-00":
    #         ## save info_amount by type
    #         json_body = [
    #             {
    #                 "measurement": "analy_result_raw_per_frame_inshot_11_4",
    #                 "tags": {
    #                     "name": str(f['name']),
    #                     "a_type": str(f['a_type']),
    #                     "day_of_week":int(f['day_of_week']),
    #                     "time_of_day":int(f['time_of_day']),
    #                 },
    #                 "fields": {
    #                     "frame_idx": int(f['frame_idx']),
    #                     "a_parameter": float(f['a_parameter']),
    #                     "fps": float(f['fps']),
    #                     "bitrate": float(f['bitrate']),
    #                     "time_consumption": float(f['time_consumption']),
    #                     "target": int(f['target'])
    #                 }
    #             }
    #         ]
    #         back_DBclient.write_points(json_body)


    # result = DBclient.query("SELECT * FROM test_shot_list")
    # result_list = list(result.get_points(measurement='test_shot_list'))
    # for r in result_list:
    #     # l = r['name'].split('/')
    #     # s = os.path.join(data['global']['base_path'], "/".join(l[1:]))
    #     json_body = [
    #                 {
    #                     "measurement": "shot_list",
    #                     "tags": {
    #                         "base_path": r['base_path'],
    #                         "storage_path": r['storage_path']
    #                     },
    #                     "fields": {
    #                         "list": r['list']
    #                     }
    #                 }
    #             ]
        
    #     DBclient.write_points(json_body,time_precision='ms')
