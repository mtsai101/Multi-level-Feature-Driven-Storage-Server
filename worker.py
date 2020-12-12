from influxdb import InfluxDBClient
import pandas as pd
DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
import csv
if __name__=='__main__':
    DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
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
    import ast
    DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
    result = list(DBclient.query("SELECT * FROM shot_list_test where \"name\"=\'./storage_server_volume/SmartPole/Pole1/2020-11-05_00-00-00/LiteOn_P1_2019-11-12_16:07:42.mp4\'"))
    
    s_list = ast.literal_eval(result[0][0]['list'])
    count = 0
    for k,r in enumerate(s_list):
        if r[0]:
            count += (s_list[k][1]-s_list[k-1][1])
    print(count)
