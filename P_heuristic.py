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

MAX_INT = pow(2,64)
ANALY_LIST=["illegal_parking0","people_counting"]

O_v = 4000# MB
delta_d = 21600
# clip_number = 6

import yaml
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)


DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', data['global']['database_name'])


pre_d_selected = None
time_matrix = None
space_matrix = None
profit_matrix = None
clip_number = 0 

result = DBclient.query('SELECT * FROM DownTimeTable')
TimeTable = pd.DataFrame(list(result.get_points(measurement="DownTimeTable")))

result = DBclient.query('SELECT * FROM DownRatioTable')
RatioTable = pd.DataFrame(list(result.get_points(measurement="DownRatioTable")))

result = DBclient.query('SELECT * FROM Degraded_Q_IATable')
Degraded_Q_IATable = pd.DataFrame(list(result.get_points(measurement="Degraded_Q_IATable")))

result = DBclient.query('SELECT * FROM MaxAnalyticTargetNumber')
MaxTargetTable = pd.DataFrame(list(result.get_points(measurement="MaxAnalyticTargetNumber")))

result = DBclient.query('SELECT * FROM down_result')
RawSizeTable = pd.DataFrame(list(result.get_points(measurement="down_result")))


def get_time_sum(pickup_quality, time_matrix):
    time_sum = 0
    for key, value in enumerate(pickup_quality):
        time_sum += time_matrix[key][value]
    return time_sum

def get_space_sum(pickup_quality, space_matrix):
    space_sum = 0
    for k, i in enumerate(pickup_quality):
        space_sum  += space_matrix[k][i]
    return space_sum
    
def get_profit_sum(pickup_quality, profit_matrix):
    profit_sum = 0
    for key, value in enumerate(pickup_quality):
        profit_sum += profit_matrix[key][value]
    return profit_sum



def P_EF(pickup_quality):
    flag = 0
    global pre_d_selected, time_matrix, space_matrix, profit_matrix, clip_number
    space_sum = get_space_sum(pickup_quality, space_matrix)
    
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


    print("EF Results :")

    time_sum = get_time_sum(pickup_quality, time_matrix) 
    space_sum = get_space_sum(pickup_quality, space_matrix)
    profit_sum = get_profit_sum(pickup_quality, profit_matrix)

    print("pickup_quality",pickup_quality)
    print("time_sum", time_sum)
    print("space_sum", space_sum)
    print("profit_sum", profit_sum)
    # pre_d_selected = np.array(pre_d_selected)
    # output_qualuity =  pre_d_selected[pickup_quality]
    # print("EF Final :", output_qualuity)

def P_EFR(pickup_quality):
    flag = 0
    global pre_d_selected, time_matrix, space_matrix, profit_matrix, clip_number
    space_sum = get_space_sum(pickup_quality, space_matrix)

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
            output_qualuity[i] = pickup_quality[i]
    
    print("EFR Results :")
    time_sum = get_time_sum(pickup_quality, time_matrix) 
    space_sum = get_space_sum(pickup_quality, space_matrix)
    profit_sum = get_profit_sum(pickup_quality, profit_matrix)

    print("pickup_quality",pickup_quality)
    print("time_sum (Have no data)", time_sum)
    print("space_sum", space_sum)
    print("profit_sum", profit_sum)
    # print("EFR Final :", output_qualuity)



def P_FIFO(pickup_quality):
    flag = 0

    global pre_d_selected, time_matrix, space_matrix, profit_matrix, clip_number
    space_sum = get_space_sum(pickup_quality, space_matrix)

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

    # pre_d_selected = np.array(pre_d_selected)
    # output_qualuity =  pre_d_selected[pickup_quality]
    # print("FIFO Final :", output_qualuity)
    print("FIFO Results :")
    time_sum = get_time_sum(pickup_quality, time_matrix) 
    space_sum = get_space_sum(pickup_quality, space_matrix)
    profit_sum = get_profit_sum(pickup_quality, profit_matrix)

    print("pickup_quality",pickup_quality)
    print("time_sum (Have no data)", time_sum)
    print("space_sum", space_sum)
    print("profit_sum", profit_sum)

def P_heuristic(pickup_quality):


    global time_matrix, space_matrix, profit_matrix, pre_d_selected, clip_number
    space_sum = get_space_sum(pickup_quality, space_matrix)

    time_matrix_sorted = np.zeros((clip_number, len(pre_d_selected)))
    space_matrix_sorted = np.zeros((clip_number, len(pre_d_selected)))
    profit_matrix_sorted = np.zeros((clip_number, len(pre_d_selected)))

    

    argsort_space_matrix = np.argsort((-space_matrix))
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
        

    time_sum = get_time_sum(pickup_quality, time_matrix_sorted) 
    space_sum = get_space_sum(pickup_quality, space_matrix_sorted)
    profit_sum = get_profit_sum(pickup_quality, profit_matrix_sorted)

    print("pickup_quality",pickup_quality)
    print("time_sum", time_sum)
    print("space_sum", space_sum)
    print("profit_sum", profit_sum)



def main():
    global pre_d_selected, time_matrix, space_matrix, profit_matrix, clip_number
    pre_d_selected = [[24,1000],[24,500],[12,500],[12,100],[6,100],[6,10],[1,10],[0,0]]

    ## Output from SLE
    length_from_SLE = [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [24, 1], [24, 1], [24, 24], [0, 0], [0, 0], [1, 1], [1, 1], [24, 1], [1, 1], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]
    ## Stored Quality
    pickup_quality = [ 0 for i in range(len(length_from_SLE))]
    pickup_quality[4] = 4

    day_list = []
    sample_info_df = None
    full_info_df = None
    for d in range(9,10):
        name = "raw_11_"+str(d)
        sample_measurement_name = "analy_sample_result_inshot_11_" + str(d)
        full_measurement_name = "analy_complete_result_inshot_11_" + str(d)

        result = DBclient.query("SELECT * FROM "+name)
        day_list.extend(list(result.get_points(measurement=name)))
        result = DBclient.query('SELECT * FROM '+sample_measurement_name)
        sample_info_df = pd.concat([sample_info_df, pd.DataFrame(list(result.get_points(measurement=sample_measurement_name)))])
        result = DBclient.query('SELECT * FROM '+full_measurement_name)
        full_info_df = pd.concat([full_info_df, pd.DataFrame(list(result.get_points(measurement=full_measurement_name)))])
    
    clip_number = len(day_list)
    time_matrix = np.zeros((clip_number, len(pre_d_selected)))
    space_matrix = np.zeros((clip_number, len(pre_d_selected)))
    profit_matrix = np.zeros((clip_number, len(pre_d_selected)))


    for i in range(time_matrix.shape[0]):
        # print(day_list[i]['name'])
        day_idx, time_idx = get_context(day_list[i]['name'])
        

        if length_from_SLE[i][0] == 0:
            ill_info = 0
        elif length_from_SLE[i][0] == 1:
            ill_info = full_info_df.loc[(full_info_df['name']==day_list[i]['name']) & (full_info_df['a_type']=='illegal_parking0')]['target'].iloc[0]
            ill_info /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]
            
        else:
            ill_info = sample_info_df.loc[(sample_info_df['name']==day_list[i]['name']) & (sample_info_df['a_type']=='illegal_parking0') & (sample_info_df['a_parameter']==str(length_from_SLE[i][0]))]['target'].iloc[0]
            ill_info/= MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]
  
        
        if length_from_SLE[i][0] == 0:
            peo_info = 0
        elif length_from_SLE[i][0] == 1:
            peo_info = full_info_df.loc[(full_info_df['name']==day_list[i]['name']) & (full_info_df['a_type']=='people_counting')]['target'].iloc[0]
            peo_info /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]
        else:
            peo_info = sample_info_df.loc[(sample_info_df['name']==day_list[i]['name']) & (sample_info_df['a_type']=='illegal_parking0') & (sample_info_df['a_parameter']==str(length_from_SLE[i][0]))]['target'].iloc[0]
            peo_info /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]
           

        target_size_row = RawSizeTable.loc[(RawSizeTable['name']==day_list[i]['name'])].iloc[0]['raw_size']
        target_time_row = TimeTable.loc[(TimeTable['day_of_week'] == str(day_idx)) & (TimeTable['time_of_day'] == str(time_idx))]
        target_ratio_row = RatioTable.loc[(RatioTable['day_of_week'] == str(day_idx)) & (RatioTable['time_of_day'] == str(time_idx))]  
        target_degraded_q_row = Degraded_Q_IATable.loc[(Degraded_Q_IATable['day_of_week'] == str(day_idx)) & (Degraded_Q_IATable['time_of_day'] == str(time_idx))]


        for j in range(time_matrix.shape[1]-1):
            
            if pickup_quality[i] == j:
                down_time = 0
                down_ratio = 1
                peo_degraded_Q_ratio = 1
                ill_degraded_Q_ratio = 1
            elif pickup_quality[i] > j:
                down_time = MAX_INT
                down_ratio = MAX_INT
                peo_degraded_Q_ratio = -MAX_INT
                ill_degraded_Q_ratio = -MAX_INT
            else:
                down_time = target_time_row.loc[(target_time_row['fps'] == str(pre_d_selected[j][0])) & (target_time_row['bitrate'] == str(pre_d_selected[j][1]))]['value']
                down_ratio = target_ratio_row.loc[(target_time_row['fps'] == str(pre_d_selected[j][0])) & (target_ratio_row['bitrate'] == str(pre_d_selected[j][1]))]['value']
                peo_degraded_Q_ratio = target_degraded_q_row.loc[(target_degraded_q_row['fps'] == str(pre_d_selected[j][0])) & (target_degraded_q_row['bitrate'] == str(pre_d_selected[j][1])) & (target_degraded_q_row['a_type'] == 'people_counting')]['value'].iloc[0]
                ill_degraded_Q_ratio = target_degraded_q_row.loc[(target_degraded_q_row['fps'] == str(pre_d_selected[j][0])) & (target_degraded_q_row['bitrate'] == str(pre_d_selected[j][1])) & (target_degraded_q_row['a_type'] == 'illegal_parking0')]['value'].iloc[0]
                
            time_matrix[i][j] = down_time 
            space_matrix[i][j] = target_size_row * down_ratio

            profit_matrix[i][j] += peo_info * peo_degraded_Q_ratio
            profit_matrix[i][j] += ill_info * ill_degraded_Q_ratio

        time_matrix[i][j+1] = 0
        space_matrix[i][j+1] = 0
        profit_matrix[i][j+1] = 0



    ###  preselected matrix
    # time_matrix = np.array([
    #     [0,45,24,12,10,3,2,0],
    #     [0,41,32,15,6,4,2,0],
    #     [0,50,23,14,8,4,1,0],
    #     [0,94,50,23,15,3,3,0],
    #     [0,46,43,35,29,6,1,0],
    #     [0,59,23,17,12,6,2,0]
    # ])

    # space_matrix = np.array([
    #     [200,100,50,40,45,23,21,0],
    #     [120,100,36,27,22,17,10,0],
    #     [250,150,74,59,45,12,8,0],
    #     [320,150,88,70,49,34,14,0],
    #     [250,74,28,24,19,15,14,0],
    #     [100,64,31,29,25,18,12,0]
    # ])

    # profit_matrix = np.array([
    #     [100,74,32,15,42,22,2,0],
    #     [94,45,30,25,15,6,32,0],
    #     [120,77,45,41,64,47,22,0],
    #     [94,54,12,45,30,19,14,0],
    #     [41,21,45,33,30,18,21,0],
    #     [45,26,33,19,26,21,15,0]
    # ])

    # pickup_quality = [
    #     0,0,0,0,0,0
    # ]

    # clip_number = len(pickup_quality)


    # P_EF(pickup_quality)
    # P_EFR(pickup_quality)
    # P_FIFO(pickup_quality)

    P_heuristic(pickup_quality)


if __name__=="__main__":
    main()