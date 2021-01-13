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
import argparse
import yaml
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)


DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', data['global']['database_name'])
result_DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', "exp_storage")

ANALY_LIST=["illegal_parking0","people_counting"]



delta_i= None # seconds

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

result = DBclient.query('SELECT * FROM Degraded_IATable')
Degraded_IATable = pd.DataFrame(list(result.get_points(measurement="Degraded_IATable")))

time_matrix = None
profit_matrix = None
pickup_length = None
clip_number = None

def L_opt(pickup_length):

    global time_matrix, profit_matrix, pre_a_selected_tuple, clip_number, delta_i

    pickup_length_knapsack = np.array([[[(len(pre_a_selected_tuple)-1) for i in range(clip_number)] for j in range(clip_number+1)] for i in range(delta_i+1)])
    opt_state = np.zeros((delta_i+1, clip_number+1))

    for delta in range(1,delta_i+1):
        for c in range(1, clip_number+1):
            # print("delta: %d, clip: %d"%(delta, c))

            remain_time_array = np.subtract(delta, time_matrix[c-1])
            candidatad_length_arg = np.argwhere(remain_time_array>=0).reshape(-1)

            # if some clip has non zero execution time
            if candidatad_length_arg.shape[0]>0:
                remain_time_idx = remain_time_array[candidatad_length_arg].reshape(-1)
                tmp_profit_matrix = list() 
                for l_i in range(len(remain_time_idx)):
                    tmp_profit_matrix.append(opt_state[remain_time_idx[l_i]][c-1] + profit_matrix[c-1][candidatad_length_arg[l_i]])
                
                tmp_max_idx = np.argmax(tmp_profit_matrix)
                tmp_max = tmp_profit_matrix[tmp_max_idx]
            else:
                tmp_max = 0
    
            if opt_state[delta][c-1] >= tmp_max: # not pickup any length of c
                opt_state[delta][c] = opt_state[delta][c-1]
                pickup_length_knapsack[delta][c] = pickup_length_knapsack[delta][c-1]
            else: # pick the length of c
                opt_state[delta][c] = tmp_max
                pickup_length_knapsack[delta][c] = pickup_length_knapsack[remain_time_idx[tmp_max_idx]][c-1]
                pickup_length_knapsack[delta][c][c-1] = candidatad_length_arg[tmp_max_idx]

        
       
    pickup_length = pickup_length_knapsack[-1][-1].astype(int)
        
    time_sum = 0
    profit_sum = 0
    for key, value in enumerate(pickup_length):
        time_sum += time_matrix[key][value]

    for key, value in enumerate(pickup_length):
        profit_sum += profit_matrix[key][value]

    print("pickup_length", pickup_length)
    print("time_sum", time_sum)
    print("profit_sum", profit_sum)
    print("profit_sum_opt", opt_state[-1][-1])

    pickup_length_transformed = []
    for i in pickup_length:
        pickup_length_transformed.append([pre_a_selected_tuple[i][0], pre_a_selected_tuple[i][1]])
    print("pickup_length_transformed", pickup_length_transformed)

    return time_sum, profit_sum, pickup_length_transformed
    
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

def L_approx(pickup_length):

    global time_matrix, profit_matrix, pre_a_selected_tuple, clip_number, delta_i
    time_matrix = time_matrix.astype(float); profit_matrix = profit_matrix.astype(float)
    eplison = 0.6; low_bound = profit_matrix.max(); up_bound = clip_number * low_bound
    x = up_bound/2
    time_matrix += 1
    ratio_matrix = profit_matrix/time_matrix
    ratio_matrix[:,-1] = 0
    prev_x = 0
    while True:
        threshold = (0.8 * x / delta_i)
        print("upper bound: ", up_bound, "low bound: ", low_bound)
        J = list()
        for c in range(clip_number):
            candidated = np.where(ratio_matrix[c]>threshold)[0]
            if candidated.shape[0]>0:
                candidated_idx = np.argmax((ratio_matrix[c])[candidated])
                select_length_idx = candidated[candidated_idx]
                J.append([c, select_length_idx, profit_matrix[c][select_length_idx]])
            else:
                continue
        # print("J", J)
        if len(J)>0:
            J_norm = np.array(J, dtype=float).sum(axis=0)[-1]
        else:
            J_norm = 0

        if J_norm <= 0.8 * x:
            up_bound = x * (1 + eplison)
            print("Adjust up_bound")
        else:
            low_bound = x * (1 - eplison)
            print(low_bound)
            print("Adjust low bound")

        
        ## Current Choice and used time
        for j in J:
            pickup_length[j[0]] = j[1]

        time_sum = 0
        for key, value in enumerate(pickup_length):
            time_sum += time_matrix[key][value]
        
        if up_bound/low_bound <= 5 and time_sum <= delta_i:
            break
        else:
            x = up_bound / 2

        if x == prev_x:
            break
        else:
            prev_x = x  

    profit_sum = 0
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

def drop_measurement_if_exist(table_name):
    result = result_DBclient.query('SELECT * FROM '+table_name)
    result_point = list(result.get_points(measurement=table_name))
    if len(result_point)>0:
        result_DBclient.query('DROP MEASUREMENT '+table_name)


if __name__=='__main__':
    
    parser = argparse.ArgumentParser(description='Parameter for L')

    parser.add_argument('-d','--delta', required=True, help='Simple Analytics Deadline (sec)')
    parser.add_argument('-a','--algo', required=True, help='Downsampling Strategy (opt, heuritstic, approx)')
    parser.add_argument('-lf','--lowfeature', required=True, help='Consider low level feature or not')

    args = parser.parse_args()

    delta_i = int(args.delta); algo = str(args.algo); use_low_feature = int(args.lowfeature)
    drop_measurement_if_exist('L_'+algo+'_exp_length')
    drop_measurement_if_exist('L_'+algo+'_exp_time_profit')

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
                target_degraded_ia_row = Degraded_IATable.loc[(Degraded_IATable['day_of_week'] == str(day_idx)) & (Degraded_IATable['time_of_day'] == str(time_idx))]
                target_time_row = TimeTable.loc[(TimeTable['day_of_week'] == str(day_idx)) & (TimeTable['time_of_day'] == str(time_idx))]

                illegal_ia = float(target_ia_row.loc[(target_ia_row['a_type'] == 'illegal_parking0')]['value'])
                people_ia = float(target_ia_row.loc[(target_ia_row['a_type'] == 'people_counting')]['value'])
                pca_value =  list(DBclient.query("SELECT * FROM visual_features_entropies_PCA_normalized where \"name\"=\'"+day_list[i]['name']+"\'"))[0][0]['value']

                illegal_time = float(target_time_row.loc[(target_time_row['a_type'] == 'illegal_parking0')]['value'])
                people_time = float(target_time_row.loc[(target_time_row['a_type'] == 'people_counting')]['value'])
                

                for j in range(time_matrix.shape[1]):
                    time_matrix[i][j] += illegal_time * (frame_num_in_shot/pre_a_selected_tuple[j][0]) if pre_a_selected_tuple[j][0] !=0 else 0
                    time_matrix[i][j] += people_time * (frame_num_in_shot/pre_a_selected_tuple[j][1]) if pre_a_selected_tuple[j][1] !=0 else 0
                    try:
                        if pre_a_selected_tuple[j][0]>1:
                            ill_ia_degrade_ratio = target_degraded_ia_row.loc[(target_degraded_ia_row['a_type']=='illegal_parking0') & (target_degraded_ia_row['a_param']==str(pre_a_selected_tuple[j][0]))]['value'].iloc[0]
                        else:
                            ill_ia_degrade_ratio = 1
                        
                        if pre_a_selected_tuple[j][1]>1:
                            peo_ia_degrade_ratio = target_degraded_ia_row.loc[(target_degraded_ia_row['a_type']=='people_counting') & (target_degraded_ia_row['a_param']==str(pre_a_selected_tuple[j][1]))]['value'].iloc[0]
                        else:
                            peo_ia_degrade_ratio = 1

                        profit_matrix[i][j] += illegal_ia * (frame_num_in_shot/pre_a_selected_tuple[j][0]) * ill_ia_degrade_ratio if pre_a_selected_tuple[j][0]!=0 else 0
                        profit_matrix[i][j] += people_ia * (frame_num_in_shot/pre_a_selected_tuple[j][1]) * peo_ia_degrade_ratio if pre_a_selected_tuple[j][1]!=0 else 0
                        
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print(exc_type, fname, exc_tb.tb_lineno, e)
                        sys.exit()
                if use_low_feature:
                    profit_matrix[i] += pca_value

            time_matrix = np.ceil(time_matrix).astype(int)

            s = time.time()
            if algo=='heuristic':
                time_sum, profit_sum, pickup_length_transformed = L_heuristic(pickup_length)
            elif algo=='opt':
                time_sum, profit_sum, pickup_length_transformed = L_opt(pickup_length)
            elif algo=='approx':
                time_sum, profit_sum, pickup_length_transformed = L_approx(pickup_length)

            exec_time = time.time() - s
            try:
                ## write to database
                for idx, day in enumerate(day_list):
                    json_body = [
                                    {
                                        "measurement":"L_"+algo+"_exp_length",
                                        "tags": {
                                            "name":str(day['name'])
                                        },
                                        "fields": {
                                            "ill_param":pickup_length_transformed[idx][0],
                                            "peo_param":pickup_length_transformed[idx][1]
                                        }
                                    }
                                ]
                    result_DBclient.write_points(json_body)

                json_body = [
                                {
                                    "measurement":"L_"+algo+"_exp_time_profit",
                                    "tags": {
                                        "day":str(d),
                                        "start":str(start),
                                        "end": str(start+5)
                                    },
                                    "fields": {
                                        "time_sum":time_sum,
                                        "profit_sum":profit_sum,
                                        "execution_time":exec_time
                                    }
                                }
                            ]
                result_DBclient.write_points(json_body)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno, e)
                sys.exit()