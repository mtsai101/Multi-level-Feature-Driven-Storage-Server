import numpy as np
import pandas as pd
import threading
import os
from threading import Lock
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision
from optimal_downsampling_manager.resource_predictor.table_estimator import IATable,AnalyTimeTable,get_context
import time
import sys
import random
import csv 

ANALY_LIST=["illegal_parking0","people_counting"]

delta_i =  900# minutes
pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]

f_c = 24*60

valid_list=[]

analyTimeTable = AnalyTimeTable(False)
iATable = IATable(False)

debug = False
trigger_interval = 6
clock = {
            "month":11,
            "day":9,
            "hour":6,
            "min_":00
        }


def L_greedy():
    start=time.time()
    DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
    
    result = DBclient.query("SELECT * FROM raw_11_"+str(clock['day']))
    result_list = list(result.get_points(measurement='raw_11_'+str(clock['day'])))
    tmp = ""
    videos=[]
    for r in result_list:
        cur_name = r['name'].split("/")[-1]
        if tmp != cur_name:
            videos.append(r['name'].split("/")[-1])
            tmp = r['name'].split("/")[-1]

    clip_list = []
    for v in videos:
        info_v = v.split("_")
        date = info_v[2].split("-")
        month = int(date[1])
        day = int(date[2])
        time_ = os.path.splitext(info_v[3])[0].split(":")
        hour = int(time_[0])
        min_ = int(time_[1])
        if month == clock["month"] and (clock["day"]-day+30)%30<1:
            diff = ((clock["hour"] + clock["min_"]/60)  - (hour + (min_/60))+24)%24
            if diff < trigger_interval:
                clip_list.append(v)
    
    clip_array = np.zeros((len(clip_list),2),dtype=np.uint8)
    for id_r, r in enumerate(clip_list):
        clip_array[id_r][0], clip_array[id_r][1] = get_context(r)
    
    if debug:
        print("Time")
        for k,r in enumerate(clip_array):
            time_dataframe = pd.DataFrame(columns=['illegal_parking0','people_counting'])
            print(result_list[k]['name'])
            for l in range(len(pre_a_selected)):
                time_list = []
                time_list.append(analyTimeTable.get_estimation(day_of_week =r[0], time_of_day = r[1], a_type=0, a_parameter=l))
                time_list.append(analyTimeTable.get_estimation(day_of_week=r[0],time_of_day=r[1], a_type=1, a_parameter=l))
                time_dataframe.loc[l] = time_list
            print(time_dataframe)
        print("---------------------------------------")
        print("IA")
        for k,r in enumerate(clip_array):
            value_dataframe = pd.DataFrame(columns=['illegal_parking0','people_counting'])
            print(result_list[k]['name'])
            for l in range(len(pre_a_selected)):
                value_list = []
                value_list.append(iATable.get_estimation(day_of_week=r[0],time_of_day=r[1], a_type=0, a_parameter=l))
                value_list.append(iATable.get_estimation(day_of_week=r[0],time_of_day=r[1], a_type=1, a_parameter=l))
                value_dataframe.loc[l] = value_list
            print(value_dataframe)


    # sys.exit()
    stime = time.time()
    target_clip_num = clip_array.shape[0]
    target_deadline = delta_i

    
    
    L =  np.array(np.zeros((target_clip_num,len(ANALY_LIST)),dtype=np.float64))
    L.fill(-1)   
    L_list = []

    W = np.ones((target_clip_num,len(ANALY_LIST)),dtype=float)
    
    print("Consider "+str(target_clip_num)+" clips")
    total_time=0
    for c in range(target_clip_num):
        for a in range(len(ANALY_LIST)):
            clip_cost_time = analyTimeTable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=len(pre_a_selected)-1)
            # Entry of L_list: [info, id_c, id_a, id_l, cost_time]
            L_list.append([
                    (iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1],a_type=a,a_parameter=len(pre_a_selected)-1)*f_c)/clip_cost_time,
                    c, 
                    a,
                    len(pre_a_selected)-1,
                    clip_cost_time
                ])
            total_time += clip_cost_time



    
    print(total_time)
    while(total_time > target_deadline):

        tmp = np.argmin(L_list)
        total_time -= L_list[tmp][-1]
        if L_list[tmp][-2] > 0:
            # decrease the length
            L_list[tmp][-2] -= 1
            L_list[tmp][-1] = analyTimeTable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=L_list[tmp][2], a_parameter=L_list[tmp][-2])

            L_list[tmp][0] = (iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1],a_type=L_list[tmp][2], a_parameter=L_list[tmp][-2])*f_c)/ L_list[tmp][-1]
            total_time += L_list[tmp][-1]
        else:    
            del L_list[tmp]

    total_info_amount = 0

    for el in L_list:
        L[el[1]][el[2]] = el[3]

        total_info_amount += iATable.get_estimation(day_of_week = clip_array[el[1]][0], time_of_day = clip_array[el[1]][1], a_type=el[2], a_parameter=el[3]) * f_c
    
    print("Final State:")
    # count = 0
    # for k,i in enumerate(L):
    #     for c in i:
    #         if c!=-1:
    #             count+=1
    #     print(clip_list[k],i)

    print("Total estimated time:", total_time)
    print("Estimation information amount:", total_info_amount)
    print("Greedy Algo Running Time at ",time.time()-stime)

    # with open("./Analy_time/Analy_timeing_time_"+str(day)+".csv",'w',newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerow([total_time])


if __name__=='__main__':
    # for day in range(9,14):
    #     for hour in range(6,30,6):
    #         clock['day'] = day
    #         clock['hour'] = hour
    L_greedy()
    