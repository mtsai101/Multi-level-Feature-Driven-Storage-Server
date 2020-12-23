from influxdb import InfluxDBClient
# from optimal_downsampling_manager.resource_predictor.table_estimator import AnalyTimeTable, DownTimeTable, DownRatioTable, IATable
import os
import csv
import ast
import yaml

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

DBclient = InfluxDBClient('172.17.0.2', data['global']['database'], 'root', 'root', 'storage')

if __name__=='__main__':
    
    # init raw video database
    for i in range(10,16):
        base_dir = "./storage_server_volume/SmartPole/Pole1/2020-11-"+str(i)+"_00-00-00"
        video_li = os.listdir(base_dir)
        video_li = sorted(video_li, key= lambda x: x)
        for v in video_li:
            v_path = os.path.join(base_dir,v)
            
            if os.path.isdir(v_path):
                continue
            json_body = [
                                {
                                    "measurement": "raw_11_"+str(i),
                                    "tags": {
                                        "name": str(v_path)
                                        
                                    },
                                    "fields": {
                                        "host": "webcamPole1"
                                    }
                                }
                            ]
            
            DBclient.write_points(json_body)

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

    # Save the shot list to databases
    # shot_list=[]
    # with open('./shot_list_4_15.csv', 'r') as csvfile:
    #     rows = csv.reader(csvfile)
    #     for row in rows:
    #         row_s = row[0].split('/')
    #         row_path = os.path.join("./storage_server_volume/SmartPole/Pole1/", os.path.join(*row_s[-2:]))
    #         shot_list.append((row_path,row[1]))


    # sorted_shot_list = sorted(shot_list, key= lambda x: x[0])
    # for s in sorted_shot_list:
    #     # print(s[0])
    #     json_body = [
    #                 {
    #                     "measurement": "shot_list",
    #                     "tags": {
    #                         "name": str(s[0])
    #                     },
    #                     "fields": {
    #                         "list": str(s[1])
    #                     }
    #                 }
    #             ]
    #     DBclient.write_points(json_body)

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

    # back_DBclient = InfluxDBClient('localhost', data['global']['database'], 'root', 'root', 'storage')

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

