import numpy as np
import pandas as pd
import threading
import os
from threading import Lock
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision
from optimal_downsampling_manager.resource_predictor.estimate_table import Full_IATable, Degraded_IATable, get_context,  AnalyTimeTable
import csv
import time
import sys
import random
from influxdb import InfluxDBClient
import yaml
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)


DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', data['global']['database_name'])

ANALY_LIST=["illegal_parking0","people_counting"]



delta_i= 21600# seconds

pre_a_selected=[1,24,48,96,144,0]
pre_a_selected_tuple = []
for a in pre_a_selected:
    for b in pre_a_selected:
        pre_a_selected_tuple.append([a,b]) 

pre_a_selected_tuple = np.array(pre_a_selected_tuple)
# print(pre_a_selected_tuple)
# analyTimeTable = AnalyTimeTable(False)
# iATable = IATable(False)
# debug = False

# time_matrix = np.array([
#     [24,10,3,12,2,0],
#     [32,15,6,4,2,0],
#     [23,14,8,4,1,0],
#     [94,23,15,3,3,0],
#     [35,29,6,15,1,0],
#     [23,17,12,6,2,0]
# ])

# profit_matrix = np.array([
#     [100,27,15,31,4,0],
#     [94,45,15,6,32,0],
#     [77,45,41,64,21,0],
#     [94,54,12,3,45,0],
#     [41,21,45,33,21,0],
#     [40,26,45,33,21,0]
# ])

# pickup_length = np.array([
#     0,0,0,0,0,0
# ])

def get_time_sum(pickup_length, time_matrix):
    time_sum = 0
    for key, value in enumerate(pickup_length):
        time_sum += time_matrix[key][value]
    return time_sum


result = DBclient.query('SELECT * FROM AnalyTimeTable')
TimeTable = pd.DataFrame(list(result.get_points(measurement="AnalyTimeTable")))

result = DBclient.query('SELECT * FROM Full_IATable')
Full_IATable = pd.DataFrame(list(result.get_points(measurement="Full_IATable")))

time_matrix = None
profit_matrix = None
pickup_length = None
clip_number = None


def L_heuristic(pickup_length):

    global time_matrix, profit_matrix, pre_a_selected_tuple, clip_number, delta_i
    argsort_time_matrix = np.argsort((-time_matrix))
    for i in range(clip_number):
        profit_matrix_sorted[i] = profit_matrix[i][argsort_time_matrix[i]]
        time_matrix_sorted[i] = time_matrix[i][argsort_time_matrix[i]]


    while get_time_sum(pickup_length, time_matrix_sorted) > delta_i:
        pending_profit_list = list() 
        for c_key, l in enumerate(pickup_length):
            if time_matrix_sorted[c_key][l] > 0:
                pending_profit_list.append((c_key, profit_matrix_sorted[c_key][l]/time_matrix_sorted[c_key][l]))

        if len(pending_profit_list)>0: # if there is other length
            victim = min(pending_profit_list, key=lambda x:x[1])

        pickup_length[victim[0]] += 1 


    for k_i, i in enumerate(pickup_length):
        pickup_length[k_i] = argsort_time_matrix[k_i][i]

    
    time_sum = 0
    profit_sum = 0
    for key, value in enumerate(pickup_length):
        time_sum += time_matrix[key][value]

    for key, value in enumerate(pickup_length):
        profit_sum += profit_matrix[key][value]

    print("time_sum", time_sum)
    print("pickup_length",pickup_length)
    print("profit_sum", profit_sum)
    pickup_length_transformed = []
    for i in pickup_length:
        pickup_length_transformed.append([pre_a_selected_tuple[i][0], pre_a_selected_tuple[i][1]])
    print("pickup_length_transformed", pickup_length_transformed)

    return time_sum, profit_sum, pickup_length_transformed
        




if __name__=='__main__':
    
    for d in range(4,16): 
        for start in range(0,23,6):
            name = "raw_11_"+str(d)
            result = DBclient.query("SELECT * FROM "+name)
            result_list = list(result.get_points(measurement=name))
            day_list = []
            for r in result_list:
                day_idx, time_idx = get_context(r['name'])
                if start<=time_idx and time_idx<start+6:
                    day_list.append(r)


            name_ = "analy_complete_result_inshot_11_"+str(d)
            result = DBclient.query('SELECT * FROM '+name_)
    
        
            ## Pending time duration
            frame_df = None
            frame_df = pd.concat([frame_df, pd.DataFrame(list(result.get_points(measurement=name_)))])

            clip_number = len(day_list)
            pickup_length = [0 for i in range(clip_number)]
            time_matrix = np.zeros((clip_number, len(pre_a_selected)*(len(pre_a_selected))))
            time_matrix_sorted = np.zeros((clip_number, len(pre_a_selected)*(len(pre_a_selected))))

            profit_matrix = np.zeros((clip_number, len(pre_a_selected)*(len(pre_a_selected))))
            profit_matrix_sorted = np.zeros((clip_number, len(pre_a_selected)*(len(pre_a_selected))))

            
            for i in range(time_matrix.shape[0]):
                # print(day_list[i]['name'])

                day_idx, time_idx = get_context(day_list[i]['name'])
                # get total of the clip

                frame_num_in_shot = frame_df.loc[frame_df['name']==day_list[i]['name']]['total_frame_number'].iloc[0]
                target_ia_row = Full_IATable.loc[(Full_IATable['day_of_week'] == str(day_idx)) & (Full_IATable['time_of_day'] == str(time_idx))]
                target_time_row = TimeTable.loc[(TimeTable['day_of_week'] == str(day_idx)) & (TimeTable['time_of_day'] == str(time_idx))]

                illegal_ia = float(target_ia_row.loc[(target_ia_row['a_type'] == 'illegal_parking0')]['value'])
                people_ia = float(target_ia_row.loc[(target_ia_row['a_type'] == 'people_counting')]['value'])
                pca_value =  list(DBclient.query("SELECT * FROM visual_features_entropies_PCA_normalized where \"name\"=\'"+day_list[i]['name']+"\'"))[0][0]['value']

                illegal_time = float(target_time_row.loc[(target_time_row['a_type'] == 'illegal_parking0')]['value'])
                people_time = float(target_time_row.loc[(target_time_row['a_type'] == 'people_counting')]['value'])

                # print(time_matrix.shape[1])
                for j in range(time_matrix.shape[1]):
                    # print(j)
                    time_matrix[i][j] += illegal_time * (frame_num_in_shot/pre_a_selected_tuple[j][0]) if pre_a_selected_tuple[j][0] !=0 else 0
                    time_matrix[i][j] += people_time * (frame_num_in_shot/pre_a_selected_tuple[j][1]) if pre_a_selected_tuple[j][1] !=0 else 0
                    profit_matrix[i][j] += illegal_ia * (frame_num_in_shot/pre_a_selected_tuple[j][0]) if pre_a_selected_tuple[j][0] !=0 else 0
                    profit_matrix[i][j] += people_ia * (frame_num_in_shot/pre_a_selected_tuple[j][1]) if pre_a_selected_tuple[j][1] != 0 else 0
                profit_matrix[i] += pca_value
                


            time_sum, profit_sum, pickup_length_transformed = L_heuristic(pickup_length)

            ## write to database
            for idx, day in enumerate(day_list):
                json_body = [
                                {
                                    "measurement":"L_heuristic_exp_length",
                                    "tags": {
                                        "name":str(day['name'])
                                    },
                                    "fields": {
                                        "ill_param":pickup_length_transformed[idx][0],
                                        "peo_param":pickup_length_transformed[idx][1]
                                    }
                                }
                            ]
                DBclient.write_points(json_body)

            json_body = [
                            {
                                "measurement":"L_heuristic_exp_time_profit",
                                "tags": {
                                    "day":str(d),
                                    "start":str(start),
                                    "end": str(start+5)
                                },
                                "fields": {
                                    "time_sum":time_sum,
                                    "profit_sum":profit_sum
                                }
                            }
                        ]
            DBclient.write_points(json_body)