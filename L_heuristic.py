import numpy as np
import pandas as pd
import threading
import os
from threading import Lock
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision
from optimal_downsampling_manager.resource_predictor.estimate_table import Full_IATable, Degraded_IATable, get_context,  AnalyTimeTable

import time
import sys
import random
from influxdb import InfluxDBClient
DBclient = InfluxDBClient('localhost', 8087, 'root', 'root', 'storage')

ANALY_LIST=["illegal_parking0","people_counting"]



delta_i= 3500# seconds
# pre_a_selected=[4000.0,2000.0,1000.0,500.0,100.0]

pre_a_selected=[1,24,48,96,144,0]
pre_a_selected_tuple = []
for a in pre_a_selected:
    for b in pre_a_selected:
        pre_a_selected_tuple.append((a,b)) 

# print(pre_a_selected_tuple)
valid_list=[]
f_c = 24 * 60 # fps * second
# analyTimeTable = AnalyTimeTable(False)
# iATable = IATable(False)
# debug = False





def get_time_sum():
    time_sum = 0
    print(pickup_length)
    for key, value in enumerate(pickup_length):
        time_sum += time_matrix[key][value]
    return time_sum


result = DBclient.query('SELECT * FROM AnalyTimeTable')
TimeTable = pd.DataFrame(list(result.get_points(measurement="AnalyTimeTable")))

result = DBclient.query('SELECT * FROM Full_IATable')
Full_IATable = pd.DataFrame(list(result.get_points(measurement="Full_IATable")))




if __name__=='__main__':
    day_list = []
    frame_df = None
    for d in range(4,5):
        name = "raw_11_"+str(d)
        name_ = "analy_complete_result_inshot_11_"+str(d)
        result = DBclient.query("SELECT * FROM "+name)
        day_list.extend(list(result.get_points(measurement=name)))
        result = DBclient.query('SELECT * FROM '+name_)
        frame_df = pd.concat([frame_df, pd.DataFrame(list(result.get_points(measurement=name_)))])



    clip_number = len(day_list)
    pickup_length = [0 for i in range(clip_number)]
    time_matrix = np.zeros((clip_number, len(pre_a_selected)*(len(pre_a_selected))))
    profit_matrix = np.zeros((clip_number, len(pre_a_selected)*(len(pre_a_selected))))

    for i in range(time_matrix.shape[0]):
        # print(day_list[i]['name'])

        day_idx, time_idx = get_context(day_list[i]['name'])
        # get total of the clip

        frame_num_in_shot = frame_df.loc[frame_df['name']==day_list[i]['name']]['total_frame_number'].iloc[0]
        target_ia_row = Full_IATable.loc[(Full_IATable['day_of_week'] == str(day_idx)) & (Full_IATable['time_of_day'] == str(time_idx))]
        target_time_row = TimeTable.loc[(TimeTable['day_of_week'] == str(day_idx)) & (TimeTable['time_of_day'] == str(time_idx))]

        illegal_ia = float(target_ia_row.loc[(target_ia_row['a_type'] == 'illegal_parking0')]['value'])
        people_ia = float(target_ia_row.loc[(target_ia_row['a_type'] == 'people_counting')]['value'])

        illegal_time = float(target_time_row.loc[(target_time_row['a_type'] == 'illegal_parking0')]['value'])
        people_time = float(target_time_row.loc[(target_time_row['a_type'] == 'people_counting')]['value'])

        # print(time_matrix.shape[1])
        for j in range(time_matrix.shape[1]):
            # print(j)
            time_matrix[i][j] += illegal_time * (frame_num_in_shot/pre_a_selected_tuple[j][0]) if pre_a_selected_tuple[j][0] !=0 else 0
            time_matrix[i][j] += people_time * (frame_num_in_shot/pre_a_selected_tuple[j][1]) if pre_a_selected_tuple[j][1] !=0 else 0
            profit_matrix[i][j] += illegal_ia * (frame_num_in_shot/pre_a_selected_tuple[j][0]) if pre_a_selected_tuple[j][0] !=0 else 0
            profit_matrix[i][j] += people_ia * (frame_num_in_shot/pre_a_selected_tuple[j][1]) if pre_a_selected_tuple[j][1] != 0 else 0

    argsort_time_matrix = np.argsort((-time_matrix))

    
    for i in range(clip_number):
        profit_matrix[i] = profit_matrix[i][argsort_time_matrix[i]]
    
    while get_time_sum() > delta_i:
        pending_profit_list = list() 
        for key, l in enumerate(pickup_length):

            if time_matrix[key][l] > 0:
                pending_profit_list.append((key, profit_matrix[key][l]/time_matrix[key][l]))

        if len(pending_profit_list)>0: # if there is other length
            victim = min(pending_profit_list, key=lambda x:x[1])

        pickup_length[victim[0]] += 1 

    print(pickup_length)
