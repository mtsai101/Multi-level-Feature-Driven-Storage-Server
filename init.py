from influxdb import InfluxDBClient
# from optimal_downsampling_manager.resource_predictor.table_estimator import AnalyTimeTable, DownTimeTable, DownRatioTable, IATable
import os
import csv
import ast
DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
if __name__=='__main__':
    # init raw video database
    # for i in range(4,10):
    #     base_dir = "./storage_server_volume/SmartPole/Pole1/2020-11-0"+str(i)+"_00-00-00"
    #     video_li = os.listdir(base_dir)
    #     video_li = sorted(video_li, key= lambda x: x)
    #     for v in video_li:
    #         v_path = os.path.join(base_dir,v)
            
    #         if os.path.isdir(v_path):
    #             continue
    #         json_body = [
    #                             {
    #                                 "measurement": "raw_11_"+str(i),
    #                                 "tags": {
    #                                     "name": str(v_path)
                                        
    #                                 },
    #                                 "fields": {
    #                                     "host": "webcamPole1"
    #                                 }
    #                             }
    #                         ]
            
    #         DBclient.write_points(json_body)

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

    # Save the shot list to databases
    shot_list=[]
    with open('./shot_list.csv', 'r') as csvfile:
        rows = csv.reader(csvfile)
        for row in rows:
            row_s = row[0].split('/')
            row_path = os.path.join("./storage_server_volume/SmartPole/Pole1/", os.path.join(*row_s[-2:]))
            shot_list.append((row_path,row[1]))


    sorted_shot_list = sorted(shot_list, key= lambda x: x[0])
    for s in sorted_shot_list:
        # print(s[0])
        json_body = [
                    {
                        "measurement": "shot_list",
                        "tags": {
                            "name": str(s[0])
                        },
                        "fields": {
                            "list": str(s[1])
                        }
                    }
                ]
        DBclient.write_points(json_body)
