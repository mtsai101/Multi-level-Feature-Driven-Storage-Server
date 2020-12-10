import numpy as np
import pandas as pd
import threading
import os
from threading import Lock
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision
from optimal_downsampling_manager.resource_predictor.table_estimator import IATable,AnalyTimeTable
from optimal_downsampling_manager.resource_predictor.table_estimator import get_context

import time
import sys
import random

ANALY_LIST=["illegal_parking0","people_counting"]

delta_i= 900# seconds

# pre_a_selected=[4000.0,2000.0,1000.0,500.0,100.0]

pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]

valid_list=[]
f_c = 24 * 60 # fps * second
analyTimeTable = AnalyTimeTable(False)
iATable = IATable(False)
debug = False
trigger_interval=6

clock = {
            "month":11,
            "day":8,
            "hour":6,
            "min_":00
        }

if __name__=='__main__':
    start=time.time()
    DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
    
    # result = DBclient.query('SELECT * FROM raw_11_8 limit 360')
    # result_point = list(result.get_points(measurement='raw_11_8'))

    result = DBclient.query("SELECT * FROM raw_11_8")
    result_list = list(result.get_points(measurement='raw_11_8'))


    clip_list = []
    for c in result_list:
        v = c['name'].split("/")[-1]
        info_v = v.split("_")
        date = info_v[2].split("-")
        month = int(date[1])
        day = int(date[2])
        time_ = os.path.splitext(info_v[3])[0].split(":")
        hour = int(time_[0])
        min_ = int(time_[1])
        if month == clock["month"] and (clock["day"]-day+30)%30<2:
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
            print(clip_list[k])

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
            print(clip_list[k])
            for l in range(len(pre_a_selected)):
                value_list = []
                value_list.append(iATable.get_estimation(day_of_week=r[0],time_of_day=r[1], a_type=0, a_parameter=l))
                value_list.append(iATable.get_estimation(day_of_week=r[0],time_of_day=r[1], a_type=1, a_parameter=l))
                value_dataframe.loc[l] = value_list
            print(value_dataframe)


    # sys.exit()


    target_clip_num = clip_array.shape[0]
    target_deadline = delta_i

    Estate = np.zeros((target_clip_num,len(ANALY_LIST),target_deadline),dtype=np.float32)
    L_time = np.zeros((target_clip_num,len(ANALY_LIST),target_deadline),dtype=np.float32)
    r = np.zeros((target_clip_num,len(ANALY_LIST)),dtype=np.int8)
    r.fill(-1)
    L =  np.array([
            [
                [
                    r for q in range(delta_i) 
                ] for j in range(len(ANALY_LIST))
            ] for i in range(target_clip_num)
        ])
    print()


    W = np.ones((target_clip_num,len(ANALY_LIST)),dtype=float)
    
    print("Consider "+str(target_clip_num)+" clips")
    s = time.time()

    for c in range(target_clip_num): 
        print(clip_list[c])
        for a in range(len(ANALY_LIST)):
            if a==0:
                if c==0:
                    last_a = 0
                    last_c = 0
                else:
                    last_a = -1
                    last_c = c-1
            else:
                last_c = c
                last_a = a-1
            
            for t in range(target_deadline):
                diff_p_comp = list()
                for l in range(len(pre_a_selected)):
                    cost_time = analyTimeTable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=l)

                    reduced_time = int(t-cost_time) 

                    
                    left_time = t - L_time[last_c][last_a][reduced_time] - cost_time

                    if left_time<0:
                        continue  
                    
                    # get estimated info amount 
                    if pre_a_selected[l]!=-1:
                        esti_info_amount = W[c][a] * iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=l) * f_c
                    else:
                        esti_info_amount = 0
                    ## weight normalized
                    # esti_info_amount /= W[c].sum()
                    
                    diff_p_comp.append((esti_info_amount,l,reduced_time,cost_time))

                if len(diff_p_comp) > 0:
                    esti_info_amount, l, reduced_time, cost_time = max(diff_p_comp)
                else:
                    continue

                if esti_info_amount + Estate[last_c][last_a][reduced_time] > Estate[last_c][last_a][t]:
                    Estate[c][a][t] = esti_info_amount + Estate[last_c][last_a][reduced_time]
                    L[c][a][t] = L[last_c][last_a][reduced_time]
                    L[c][a][t][c][a] = l
                    L_time[c][a][t] = L_time[last_c][last_a][reduced_time] + cost_time
                else:
                    Estate[c][a][t] = Estate[last_c][last_a][t]
                    L[c][a][t] = L[last_c][last_a][t]
                    L_time[c][a][t] = L_time[last_c][last_a][t]
                    

    count=0
    for k,r in enumerate(L[-1][-1][-1]):
        for c in r:
            if c!=-1:
                count+=1
        print(clip_list[k],r)
    print(count)
    print("Estimated execution time:",L_time[-1][-1][-1])
    print("Estimated info:",Estate[-1][-1][-1])
    print("Algo running time:",time.time()-s)


    
    # max_info = 0
    # max_idx = (-1,-1,-1,-1,-1,-1)
    # cost_time = 0
    # pre_a_selected.append(-1)
    # for i in range(len(pre_a_selected)):
    #     if i == len(pre_a_selected)-1:
    #         ia11=0
    #     else:
    #         ia11 = W[c][0] * iATable.get_estimation(day_of_week=clip_array[0][0],time_of_day=clip_array[0][1], a_type=0, a_parameter=i)* pre_a_selected[i] 
    #     for j in range(len(pre_a_selected)):

    #         if j == len(pre_a_selected)-1:
    #             ia12=0
    #         else:
    #             ia12 = W[c][1] * iATable.get_estimation(day_of_week=clip_array[0][0],time_of_day=clip_array[0][1], a_type=1, a_parameter=j)* pre_a_selected[j] 

    #         ia1 = ia11+ia12

    #         for k in range(len(pre_a_selected)):
    #             if k == len(pre_a_selected)-1:
    #                 ia21=0
    #             else:
    #                 ia21 = W[c][0] * iATable.get_estimation(day_of_week=clip_array[1][0],time_of_day=clip_array[1][1], a_type=0, a_parameter=k)* pre_a_selected[k] 
    #             for l in range(len(pre_a_selected)):
    #                 if l == len(pre_a_selected)-1:
    #                     ia22=0
    #                 else:
    #                     ia22 = W[c][1] * iATable.get_estimation(day_of_week=clip_array[1][0],time_of_day=clip_array[1][1], a_type=1, a_parameter=l)* pre_a_selected[l] 

    #                 ia2 = ia21+ia22

    #                 for r in range(len(pre_a_selected)):
    #                     if r == len(pre_a_selected)-1:
    #                         ia31=0
    #                     else:
    #                         ia31 = W[c][0] * iATable.get_estimation(day_of_week=clip_array[2][0],time_of_day=clip_array[2][1], a_type=0, a_parameter=r)* pre_a_selected[r] 
    #                     for s in range(len(pre_a_selected)):
    #                         if s == len(pre_a_selected)-1:
    #                             ia32=0
    #                         else:
    #                             ia32 = W[c][1] * iATable.get_estimation(day_of_week=clip_array[2][0],time_of_day=clip_array[2][1], a_type=1, a_parameter=s)* pre_a_selected[s] 
    #                         ia3 = ia31+ia32
    #                         if ia1+ia2+ia3 > max_info:
    #                             time=0
    #                             tmp = (i,j,k,l,r,s)
    #                             for id_a, p in enumerate(tmp):
    #                                 if p==len(pre_a_selected)-1:
    #                                     continue
    #                                 time += analyTimeTable.get_estimation(day_of_week=clip_array[int(id_a/2)][0],time_of_day=clip_array[int(id_a/2)][1], a_type = id_a%2, a_parameter = p)
    #                             if time < delta_i:
    #                                 max_info = ia1+ia2+ia3
    #                                 max_idx = tmp
    #                                 cost_time = time
    #                             else:
    #                                 continue
    # print("Brute force:")
    # print("Info:", max_info)
    # print("lengths:", max_idx)
    # print("Time:", cost_time)


    