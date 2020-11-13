from influxdb import InfluxDBClient
import numpy as np
import random
import csv 
import os
from optimal_downsampling_manager.resource_predictor.table_estimator import IATable,DownTimeTable,DownRatioTable,get_context,drop_measurement_if_exist




iATable = IATable(False)


f_c = 24*60
ANALY_LIST = ["people_counting","illegal_parking0"]
pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]
pre_frame_rate = [24.0,12.0,6.0,1.0]
pre_bitrate = [1000.0, 500.0, 100.0, 10.0]
pre_d_selected = []

for f in pre_frame_rate:
    for b in pre_bitrate:
        pre_d_selected.append([f,b])

pre_d_selected=np.array(pre_d_selected) 

DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'video_edge')

max_info = dict()
for a_type in ANALY_LIST:
        max_result = DBclient.query('SELECT target/a_parameter AS info_amount FROM analy_result WHERE a_type=\''+a_type+'\'')
        max_result = list(max_result.get_points(measurement="analy_result"))
        max_value = sorted(max_result, key=lambda k :k['info_amount'],reverse=True)[0]['info_amount']
        max_info[a_type] = max_value


start_day = 9
end_day = 13

if __name__=='__main__':


    # universal_dataset = []
    # for r in range(start_day,end_day+1):
    #     result = DBclient.query("SELECT * FROM raw_11_"+str(r)+"_analy_all")
    #     universal_dataset_per_day = list(result.get_points(measurement="raw_11_"+str(r)+"_analy_all"))
    #     hour_list = [[] for i in range(24)]
    #     for c in universal_dataset_per_day:
    #         hour = int(c['name'].split("/")[-1].split("_")[-1].split(":")[0])
    #         hour_list[hour].append(c['name'])
    #     universal_dataset.append(hour_list)


    # for sim_day in range(start_day, end_day+1):
    #     pending_list = []
    #     size  = (sim_day-start_day+1)*24
    #     peo_rate = np.random.poisson(10,size) # 10 request / hour 
    #     with open('./query_pending_list/'+str(sim_day)+'.csv','w',newline='') as f:
    #         writer = csv.writer(f)

    #         for k_p, p in enumerate(peo_rate):
    #             tmp_list = universal_dataset[int(k_p/24)][k_p%24]
    #             pending_list.extend(random.sample(tmp_list,k=min(len(tmp_list),p)))
    #         for pend in pending_list:
    #             writer.writerow([pend])



    

    # print("Init DB")
    # dblist = []
    # for d in dir_:
    #     db_file = os.path.join(path,d)
    #     db=[]
    #     if d =='estimate_info.csv':
    #         continue
    #     day_stamp = 0
    #     time_stamp = 0
    #     with open(db_file,'r',newline='') as f:
    #         rows = csv.reader(f)

    #         for r in rows:
    #             if len(r)< 2:
    #                 continue
    #             tmp_day = int(r[0].split("/")[-1].split("_")[-2].split("-")[-1])
    #             db.append(r)
    #             if day_stamp <= tmp_day:
    #                 day_stamp = tmp_day
    #                 tmp_time = int(r[0].split("/")[-1].split("_")[-1].split(":")[0])
    #                 if time_stamp<=tmp_time:
    #                     time_stamp = tmp_time

    #         dblist.append([day_stamp,time_stamp,db])


    type_ = 'greedy_20'
    print("Updating the database...")
    drop_measurement_if_exist("videos_in_server_simulation")
    db=[]
    with open('./prob2_'+type_+'/clips_13_23.csv','r',newline='') as f:
        rows = csv.reader(f)
        for r in rows:
            json_body = [
                        {
                            "measurement": "videos_in_server_simulation",
                            "tags": {
                                "name": str(r[0]),
                                "host": "webcamPole1"
                            },
                            "fields": {
                                "fps":float(r[1]),
                                "bitrate":float(r[2]),
                            }
                        }
                    ]
            DBclient.write_points(json_body)

    
    print("Quering information amount...")
    with open('./query_pending_list/13.csv','r',newline='') as f:
        rows = csv.reader(f)
        

        for row in rows:
            ia=0
            day = int(row[0].split("/")[-2].split("_")[-1])
            hour_time = int(row[0].split("/")[-1].split("_")[-1].split(":")[0])
            print(day,hour_time)
            query_new_name = '/home/min/ssd/space_experiment/'+row[0].split("/")[-2]+'/'+row[0].split("/")[-1]

            pending_clip = DBclient.query("SELECT * from videos_in_server_simulation where \"name\"=\'"+query_new_name+"\'")
            pending_clip = list(pending_clip.get_points(measurement="videos_in_server_simulation"))

            for atype in ANALY_LIST:

                if len(pending_clip)>0:
                    clip = pending_clip[0]
                    if float(clip['fps'])==24.0 and float(clip['bitrate'])== 1000.0:
                        print("raw")
                        date = clip['name'].split("/")[-2].split("/")[-1]
                        tmp_result = DBclient.query("SELECT * FROM "+date+"_analy_all WHERE \"name\"=\'"+row[0]+"\' AND a_type=\'"+atype+"\'")
                        tmp_result = list(tmp_result.get_points(measurement=date+"_analy_all"))
                        ia += ((tmp_result[0]['target']/tmp_result[0]['a_parameter']) / max_info[atype]) * tmp_result[0]['a_parameter']
                    else:
                        print("use pred")
                        fps = pre_frame_rate.index(int(clip['fps']))
                        bitrate = pre_bitrate.index(int(clip['bitrate']))
                        p_id=fps * len(pre_frame_rate) + bitrate

                        day_of_week, time_of_day = get_context(clip['name'])
                        a_id = ANALY_LIST.index(atype)
                        ia +=  iATable.get_estimation(day_of_week=day_of_week,time_of_day=time_of_day,a_type=a_id,p_id=p_id) * f_c
                else:
                    ia += 0

            with open('./query_result/'+type_+'/'+str(day)+'.csv','a',newline='') as cw:
                writer = csv.writer(cw)
                writer.writerow([ia])






            
            




        
            
    

            


        
