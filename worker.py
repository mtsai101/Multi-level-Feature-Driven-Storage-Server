from influxdb import InfluxDBClient
import pandas as pd
from optimal_downsampling_manager.resource_predictor.estimate_table import Full_IATable, Degraded_IATable, get_context, drop_measurement_if_exist, AnalyTimeTable, DownTimeTable, DownRatioTable, Degraded_Q_IATable
# DBclient = InfluxDBClient('localhost', 8087, 'root', 'root', 'storage')
import csv
import cv2
import ast 
import sys
import os
if __name__=='__main__':
    # result = list(DBclient.query("SELECT * FROM analy_result_raw_complete"))
    # with open("raw_11_5.csv", 'w') as csvfile:
    #     writer = csv.writer(csvfile)

    #     for r in result[0]:
    #         if r['a_type'] =='people_counting':
    #             writer.writerow([r['name'],r['target'],r['time_consumption']])

    # json_body=[]
    # for f in range(1,5000):
    #     #save info_amount by type
    #     json_body=[{
    #             "measurement": "test",
    #             "tags": {
    #                 "name": str(f)+"hello",
    #                 "host": "webcamPole1"
    #             },
    #             "fields": {
    #                 "frame_idx": int(f)
    #             }
    #         }
    #     ]
    #     DBclient.write_points(json_body)
    # DBclient.write_points(json_body, database='storage', time_precision='ms', batch_size=40000, protocol='json')
    
    
    """
        Measurement shot length
    """
    # import ast
    # DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
    # result = list(DBclient.query("SELECT * FROM shot_list_test where \"name\"=\'./storage_server_volume/SmartPole/Pole1/2020-11-05_00-00-00/LiteOn_P1_2019-11-12_16:07:42.mp4\'"))
    
    # s_list = ast.literal_eval(result[0][0]['list'])
    # count = 0
    # for k,r in enumerate(s_list):
    #     if r[0]:
    #         count += (s_list[k][1]-s_list[k-1][1])
    # print(count)

    # result = DBclient.query("SELECT * FROM raw_11_4,raw_11_5")
    # result_list = list(result.get_points(measurement="raw_11_4"))
    # import json

    # DBclient.write(json.dumps(result_list), params={"database":'test', "measurement":'video_in_server'}, protocol='json')
    # print(json.dumps(result_list))

    # full_IATable = Full_IATable(True)
    # degraded_IATable = Degraded_IATable(True)
    # analyTimeTable = AnalyTimeTable(True)
    # downTimeTable = DownTimeTable(True)
    # downRatioTable = DownRatioTable(True)
    # degraded_Q_IATable = Degraded_Q_IATable(True)


    ## build degrade L_ia data for degrade L_ia Table
    # sample_length_list = [24,48,96,144]
    # a_type_list = ['illegal_parking0','people_counting']
    # for day in range(4,16):
    #     name = "raw_11_"+str(day)
    #     result = DBclient.query("SELECT * FROM "+name)
    #     day_list = list(result.get_points(measurement=name))

    #     for day_all in day_list:
    #         path = day_all['name']
    #         day_idx, time_idx = get_context(path)
    #         print("querying shot")    
    #         result = DBclient.query("SELECT * FROM shot_list where \"name\"=\'"+path+"\'")
    #         shot_list = ast.literal_eval(list(result.get_points(measurement='shot_list'))[0]['list'])

    #         for a_type in a_type_list:
    #             print("querying frame")
                
    #             each_frame_result = DBclient.query("SELECT * FROM analy_result_raw_per_frame_inshot_11_"+str(day)+" where \"name\"=\'"+path+"\' AND \"a_type\"=\'"+a_type+"\'")
    #             each_frame_result = list(each_frame_result.get_points(measurement = "analy_result_raw_per_frame_inshot_11_"+str(day)))
                
    #             fps = 24; bitrate = 1000
    #             print("finish querying")
    #             print("start to sorting")
    #             sorted_frame_result = sorted(each_frame_result, key=lambda k :int(k['frame_idx']))

    #             for sample_length in sample_length_list:
    #                 frame_counter = 0
    #                 sample_buf = sample_length
    #                 target_counter = 0
    #                 total_time_consumption = 0
    #                 total_frame_num = 0
    #                 print("start to capture")
    #                 print("There are ",len(sorted_frame_result)," frames in ", path)

    #                 for frame in sorted_frame_result:
    #                     # print(frame['frame_idx'])
    #                     sample_buf -= 1
    #                     if sample_buf>0:
    #                         continue
    #                     else:
    #                         sample_buf = sample_length
                        
    #                     target_counter += frame['target']
    #                     total_frame_num += 1
    #                     total_time_consumption += frame['time_consumption']

    #                 print("start save to db")


    #                 json_body=[
    #                         {
    #                             "measurement": "analy_sample_result_inshot_11_"+str(day),
    #                                 "tags": {
    #                                     "a_type": str(a_type),
    #                                     "day_of_week":int(day_idx),
    #                                     "time_of_day":int(time_idx),
    #                                     "a_parameter": int(sample_length), 
    #                                     "fps": int(fps),
    #                                     "bitrate": int(bitrate)
    #                                 },
    #                                 "fields": {
    #                                     "total_frame_number":int(total_frame_num),
    #                                     "name": str(path),
    #                                     "time_consumption": float(total_time_consumption),
    #                                     "target": int(target_counter)
    #                                 }
    #                         }
    #                 ]
    #                 DBclient.write_points(json_body)
    #                 print("Finish "+str(path)+" at sample length = "+str(sample_length) +"on "+a_type)
                        
        

    # path = "/home/min/Analytic-Aware_Storage_Server/storage_server_volume/converted_videos/1-10/2020-11-04_00-00-00/Pole1_2020-11-04_12-00-00.mp4"
    # path = "/home/min/Analytic-Aware_Storage_Server/storage_server_volume/SmartPole/Pole1/2020-11-04_00-00-00/Pole1_2020-11-04_12-00-00.mp4"
    # cap = cv2.VideoCapture(path)
    # print(int(cap.get(cv2.CAP_PROP_FRAME_COUNT)))


    ### scp files
    # for i in range(4,12):
    #     database = 'analy_result_sample_quality_frame_inshot_11_'+str(i)
    #     drop_measurement_if_exist(database)
    
    # for i in range(4,12):
    #     database = 'analy_complete_sample_quality_result_inshot_11_'+str(i)
    #     drop_measurement_if_exist(database)


    ## Move measurement from storage to merge_storage
    # DBclient = InfluxDBClient('localhost', 8087, 'root', 'root', 'storage')
    # result = DBclient.query("SELECT * FROM analy_complete_sample_quality_result_inshot_11_6")
    # result_list = list(result.get_points(measurement = "analy_complete_sample_quality_result_inshot_11_6"))
    # json_body = []
    # with open("rsample_9_all.csv", 'r') as csvfile:
    #     rows = csv.reader(csvfile)
    #     for k,i in enumerate(rows):
    #         if k==0:
    #             continue

    #         json_body.append(
    #                     {
    #                         "measurement": "analy_complete_sample_quality_result_inshot_11_9",
    #                         "tags": {
    #                             "a_type": str(i[0]),
    #                             "day_of_week":int(i[1]),
    #                             "time_of_day":int(i[2]),
    #                             "a_parameter": int(i[3]), 
    #                             "fps": int(i[4]),
    #                             "bitrate": int(i[5])
    #                         },
    #                         "fields": {
    #                             "total_frame_number":int(i[6]),
    #                             "name": str(i[7]),
    #                             "time_consumption": float(i[8]),
    #                             "target": int(i[9])
    #                         }
    #                     }
    #         )

    # DBclient = InfluxDBClient('localhost', 8087, 'root', 'root', 'merge_storage')        
    # DBclient.write_points(json_body, database='merge_storage', time_precision='ms', batch_size=40000, protocol='json')



    ## rename PCA
    # DBclient = InfluxDBClient('localhost', 8087, 'root', 'root', 'merge_storage')
    # result = DBclient.query("SELECT * FROM visual_features_entropy_PCA_normalized")
    # result_list = list(result.get_points(measurement = "visual_features_entropy_PCA_normalized"))
    # data_points = []

    # for r in result_list:
    #     data_points.append({
    #         "measurement": "visual_features_entropies_PCA_normalized",
    #         "tags": {
    #             "name": str(r['name'][1:])
    #         },
    #         "fields": {
    #             "value": float(r['value'])
    #         }
    #     })
    
    # DBclient.write_points(data_points, database='merge_storage', time_precision='ms', batch_size=300, protocol='json')  
    # name="./storage_server_volume/SmartPole/Pole1/2020-11-15_00-00-00/Pole1_2020-11-15_20-00-00.mp4"
    # pca_value =  list(DBclient.query("SELECT * FROM visual_features_entropies_PCA_normalized where \"name\"=\'"+name+"\'"))[0][0]['value']
    # print(pca_value)


    ### Update data_points testing
    # DBclient = InfluxDBClient('localhost', 8087, 'root', 'root', 'merge_storage')
    # name= "water"
    # r = list(DBclient.query("SELECT * FROM meters where \"meter\"=\'"+name+"\'"))[0][0]
    # print(r)

    # json_body = [
    #             {
    #                 "measurement":"meters",
    #                 "tags": {
    #                     "meter": name,
    #                     "place":"6F-1"
    #                 },
    #                 "time":r['time'],
    #                 "fields": {
    #                     "consumption":8.9,
    #                     "volume":112.456
    #                 }
    #             }
    #         ]
    # DBclient.write_points(json_body, time_precision='ms')



    ### Convert video path from ../converted/... to original
    # DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'merge_storage')
    # for d in range(5, 16):
    #     result = list(DBclient.query("SELECT * FROM analy_complete_sample_quality_result_inshot_11_"+str(d)))[0]
    #     for r in result:
    #         new_path = os.path.join("./storage_server_volume/SmartPole/Pole1", "/".join(r['name'].split("/")[-2:]))
    #         json_body = [
    #                 {
    #                     "measurement": "sample_quality_alltarget_inshot_11_"+str(d),
    #                     "tags": {
    #                         "a_type": str(r['a_type']),
    #                         "day_of_week":int(r['day_of_week']),
    #                         "time_of_day":int(r['time_of_day']),
    #                         "a_parameter": int(r['a_parameter']), 
    #                         "fps": int(r['fps']),
    #                         "bitrate": int(r['bitrate'])
    #                     },
    #                     "fields": {
    #                         "total_frame_number":int(r['total_frame_number']),
    #                         "name": str(new_path),
    #                         "time_consumption": float(r['time_consumption']),
    #                         "target": int(r['target'])
    #                     }
    #                 }
    #             ]
    #         DBclient.write_points(json_body)
            