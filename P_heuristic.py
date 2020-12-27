import numpy as np
import pandas as pd
import threading
import os
from threading import Lock
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision
from optimal_downsampling_manager.resource_predictor.estimate_table import Full_IATable, Degraded_IATable, get_context, DownTimeTable, DownRatioTable, Degraded_Q_IATable

import time
import sys
import random

ANALY_LIST=["illegal_parking0","people_counting"]

O_v = 600# MB
delta_d = 200
clip_number = 6
# pre_a_selected=[4000.0,2000.0,1000.0,500.0,100.0]

pre_d_selected = [[24,1000],[24,500],[12,500],[12,100],[6,100],[6,10],[1,10],[0,0]]


time_matrix = np.array([
    [0,45,24,12,10,3,2,0],
    [0,41,32,15,6,4,2,0],
    [0,50,23,14,8,4,1,0],
    [0,94,50,23,15,3,3,0],
    [0,46,43,35,29,6,1,0],
    [0,59,23,17,12,6,2,0]
])

space_matrix = np.array([
    [200,100,50,40,45,23,21,0],
    [120,100,36,27,22,17,10,0],
    [250,150,74,59,45,12,8,0],
    [320,150,88,70,49,34,14,0],
    [250,74,28,24,19,15,14,0],
    [100,64,31,29,25,18,12,0]
])

profit_matrix = np.array([
    [100,74,32,15,42,22,2,0],
    [94,45,30,25,15,6,32,0],
    [120,77,45,41,64,47,22,0],
    [94,54,12,45,30,19,14,0],
    [41,21,45,33,30,18,21,0],
    [45,26,33,19,26,21,15,0]
])

pickup_quality = [
    0,0,0,0,0,0
]

def get_time_sum():
    time_sum = 0
    print(pickup_length)
    for key, value in enumerate(pickup_length):
        time_sum += time_matrix[key][value]
    return time_sum

space_sum = 0
for k, i in enumerate(pickup_quality):
    space_sum  += space_matrix[k][i]

time_sum = 0

# result = DBclient.query('SELECT * FROM DownTimeTable')
# TimeTable = pd.DataFrame(list(result.get_points(measurement="DownTimeTable")))

# result = DBclient.query('SELECT * FROM DownRatioTable')
# RatioTable = pd.DataFrame(list(result.get_points(measurement="DownRatioTable")))

# result = DBclient.query('SELECT * FROM Degraded_Q_IATable')
# Degraded_Q_IATable = pd.DataFrame(list(result.get_points(measurement="Degraded_Q_IATable")))

# result = DBclient.query('SELECT * FROM MaxAnalyticTargetNumber')
# MaxTargetTable = pd.DataFrame(list(result.get_points(measurement="MaxAnalyticTargetNumber")))

# result = DBclient.query('SELECT * FROM down_result')
# RawSizeTable = pd.DataFrame(list(result.get_points(measurement="down_result")))

# length_from_SLE = [(a_param_peo, a_param_ill)]

def P_EF():
    flag = 0
    global space_sum, time_sum, pre_d_selected
    # print("init ", pickup_quality, space_sum)
    while space_sum > O_v:
        # print(flag)
        if np.all(np.array(pickup_quality)==len(pre_d_selected)-1):
            break

        flag = flag%len(pickup_quality)
        if pickup_quality[flag] == 0: # make the quality to (24, 500)
            pickup_quality[flag] = 1
            space_sum = space_sum - space_matrix[flag][0] + space_matrix[flag][1]
        elif pickup_quality[flag] == len(pre_d_selected)-1:
            flag+=1
            continue
        else: # delete the file
            space_sum = space_sum - space_matrix[flag][pickup_quality[flag]]
            pickup_quality[flag] = len(pre_d_selected)-1

        
        # print(pickup_quality,space_sum)
        flag += 1

    pre_d_selected = np.array(pre_d_selected)
    output_qualuity =  pre_d_selected[pickup_quality]
    print("EF Final :", output_qualuity)

def P_EFR():
    flag = 0
    global space_sum, time_sum, pre_d_selected
    
    # print("init ", pickup_quality, space_sum)
    while space_sum > O_v:
        # print(flag)
        if np.all(np.array(pickup_quality)==len(pre_d_selected)-1):
            break

        flag = flag%len(pickup_quality)
        if pickup_quality[flag] == 0: # make the quality to (6, 1000)
            pickup_quality[flag] = -1 
            space_sum = space_sum - space_matrix[flag][0] * 0.75
        elif pickup_quality[flag] == len(pre_d_selected)-1:
            flag+=1
            continue
        else:
            if pickup_quality[flag] == -1: ## has been degrade, need to be deleted / already is 6 fps, delete it
                space_sum = space_sum - space_matrix[flag][0] * 0.25
                pickup_quality[flag] = len(pre_d_selected)-1

            elif pre_d_selected[pickup_quality[flag]][0] == 6:
                space_sum = space_sum - space_matrix[flag][pickup_quality[flag]]
                pickup_quality[flag] = len(pre_d_selected)-1

            else: # degrade to the 6 fps
                space_sum = space_sum - space_matrix[flag][pickup_quality[flag]] + space_matrix[flag][0]*0.25
                pickup_quality[flag] = -1
            

        
        # print(pickup_quality,space_sum)
        flag += 1

    output_qualuity = [-2 for i in range(clip_number)]
    
    for i in range(clip_number):
        if pickup_quality[i] == -1: # the quality has been degrade
            output_qualuity[i] = [6,1000]
        else:
            output_qualuity[i] = pre_d_selected[pickup_quality[i]]

    print("EFR Final :", output_qualuity)


def P_FIFO():
    flag = 0
    global space_sum, time_sum, pre_d_selected
    # print("init ", pickup_quality, space_sum)
    while space_sum > O_v:
        if np.all(np.array(pickup_quality)==len(pre_d_selected)-1):
            break

        flag = flag%len(pickup_quality)

        if pickup_quality[flag] == len(pre_d_selected)-1:
            flag+=1
            continue
        else: # delete the file
            space_sum = space_sum - space_matrix[flag][pickup_quality[flag]]
            pickup_quality[flag] = len(pre_d_selected)-1

        
        # print(pickup_quality,space_sum)
        flag += 1

    pre_d_selected = np.array(pre_d_selected)
    output_qualuity =  pre_d_selected[pickup_quality]
    print("FIFO Final :", output_qualuity)

def P_heuristic():
    global pre_d_selected, space_sum, space_matrix, profit_matrix
    argsort_space_matrix = np.argsort((-space_matrix))

    time_matrix_sorted = np.zeros((clip_number, len(pre_d_selected)))
    space_matrix_sorted = np.zeros((clip_number, len(pre_d_selected)))
    profit_matrix_sorted = np.zeros((clip_number, len(pre_d_selected)))


    for i in range(clip_number):
        time_matrix_sorted[i] = time_matrix[i][argsort_space_matrix[i]]
        space_matrix_sorted[i] = space_matrix[i][argsort_space_matrix[i]]
        profit_matrix_sorted[i] = profit_matrix[i][argsort_space_matrix[i]]



    profit_list = []
    for c, q in enumerate(pickup_quality):
        s = space_matrix_sorted[c][q]

        if s>0 and pickup_quality[c] < profit_matrix_sorted.shape[1]:
            profit_list.append((c, profit_matrix_sorted[c][q]/s)) 

    while space_sum > O_v or time_sum > delta_d:
        
        victim_c = min(profit_list, key= lambda x: x[1])
        # print(profit_list)


        c = victim_c[0]
        profit_list.remove(victim_c)
        d = pickup_quality[c] + 1
        
        
        if pickup_quality[c]==space_matrix_sorted.shape[1]-1:
            print("the victim can not be downsample anymore")
            continue

        
        space_sum = space_sum - space_matrix_sorted[c][pickup_quality[c]] + space_matrix_sorted[c][d] 
        
        time_sum = 0
        for t_key,v in enumerate(pickup_quality):
            time_sum += time_matrix_sorted[t_key][v]


        s = space_matrix_sorted[c][d]
        if s > 0: ## make sure the last one is zero
            profit_list.append((c, profit_matrix_sorted[c][d]/s)) 

        pickup_quality[c] = d
        # print(c, pickup_quality[c])
        if len(profit_list) == 0:
            print("np video")
            break
        # print(pickup_quality)
       
        # print(space_sum, time_sum)

    pre_d_selected = np.array(pre_d_selected)
    for k_i, i in enumerate(pickup_quality):
        pickup_quality[k_i] = argsort_space_matrix[k_i][i]
        

    time_sum = 0
    for key, value in enumerate(pickup_quality):
        time_sum += time_matrix[key][value]

    space_sum = 0
    for key, value in enumerate(pickup_quality):
        space_sum += space_matrix[key][value]

    profit_sum = 0
    for key, value in enumerate(pickup_quality):
        profit_sum += profit_matrix[key][value]

    print("pickup_quality",pickup_quality)
    print("time_sum", time_sum)
    print("space_sum", space_sum)
    print("profit_sum", profit_sum)




if __name__=='__main__':
    # P_EF()
    # P_EFR()
    # P_FIFO()
    P_heuristic()
    # day_list = []
    # estimate_info_df = None
    # for d in range(4,5):
    #     name = "raw_11_"+str(d)
    #     name_ = "analy_sample_result_inshot_11_"+str(d)
    #     result = DBclient.query("SELECT * FROM "+name)
    #     day_list.extend(list(result.get_points(measurement=name)))
    #     result = DBclient.query('SELECT * FROM '+name_)
    #     estimate_info_df = pd.concat([estimate_info_df, pd.DataFrame(list(result.get_points(measurement=name_)))])


    # clip_number = len(day_list)
    # pickup_length = [0 for i in range(clip_number)]
    # time_matrix = np.zeros((clip_number, len(pre_d_selected))))
    # space_matrix = np.zeros((clip_number, len(pre_d_selected)))
    # profit_matrix = np.zeros((clip_number, len(pre_d_selected)))


    # for i in range(time_matrix.shape[0]):
    #     print(day_list[i]['name'])

    #     day_idx, time_idx = get_context(day_list[i]['name'])
        
    #     people_by_estimated_length = estimate_info_df.loc[(estimate_info_df['name']==day_list[i]['name']) & (estimate_info_df['a_type']=='people_counting') & (estimate_info_df['a_parameter']==length_from_SLE[j][0])]['target'].iloc[0]/MaxTargetTable.loc[(MaxAnalyticTargetNumber['a_type']=='people_counting')]
    #     ill_by_estimated_length = estimate_info_df.loc[(estimate_info_df['name']==day_list[i]['name']) & (estimate_info_df['a_type']=='illegal_parking0') & (estimate_info_df['a_parameter']==length_from_SLE[j][1])]['target'].iloc[0]/MaxTargetTable.loc[(MaxAnalyticTargetNumber['a_type']=='illegal_parking0')]

    #     target_size_row = RawSizeTable.loc[(RawSizeTable['name']==day_list[i]['name'])].iloc[0]['raw_size']
    #     target_time_row = TimeTable.loc[(TimeTable['day_of_week'] == str(day_idx)) & (TimeTable['time_of_day'] == str(time_idx))]
    #     target_ratio_row = RatioTable.loc[(RatioTable['day_of_week'] == str(day_idx)) & (RatioTableRatioTable['time_of_day'] == str(time_idx))]   

        # target_degraded_q_row = Degraded_Q_IATable.loc[(RatioTable['day_of_week'] == str(day_idx)) & (RatioTableRatioTable['time_of_day'] == str(time_idx))]
        

        # for j in range(time_matrix.shape[1]-1):
        #     down_time = float(target_time_row.loc[(target_time_row['fps'] == pre_d_selected[j][0]) & (target_time_row['bitrate'] == pre_d_selected[j][1])]['value'])
        #     time_matrix[i][j] = down_time 
            
        #     down_ratio = float(target_ratio_row.loc[(target_time_row['fps'] == pre_d_selected[j][0]) & (target_ratio_row['bitrate'] == pre_d_selected[j][1])]['value'])
        #     space_matrix[i][j] = target_size_row * down_ratio


    #         peo_degraded_Q_ratio = float(target_degraded_q_row.loc[(target_time_row['fps'] == pre_d_selected[j][0]) & (target_ratio_row['bitrate'] == pre_d_selected[j][1]) & (target_ratio_row['a_type'] == 'people_counting')]['value'])
    #         ill_degraded_Q_ratio = float(target_degraded_q_row.loc[(target_time_row['fps'] == pre_d_selected[j][0]) & (target_ratio_row['bitrate'] == pre_d_selected[j][1]) & (target_ratio_row['a_type'] == 'illegal_parking0')]['value']) 

            
    #         profit_matrix[i][j] += people_by_estimated_length * peo_degraded_Q_ratio
    #         profit_matrix[i][j] += ill_by_estimated_length * ill_degraded_Q_ratio
        
    #     time_matrix[i][j] = 0
    #     space_matrix[i][j] = 0
    #     profit_matrix[i][j] = 0
