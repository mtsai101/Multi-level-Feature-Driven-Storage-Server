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
765772

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
pickup_quality_init = pickup_quality
pickup_info = [0 for i in range(clip_number)]
for k, i in enumerate(pickup_quality):
    if k==0:
        pickup_info[k] = profit_matrix[0][i]
    else:
        pickup_info[k] = profit_matrix[k][i] + pickup_info[k-1]
        
pickup_space = [0 for i in range(clip_number)]
for k, i in enumerate(pickup_quality):
    if k==0:
        pickup_space[k] = space_matrix[0][i]
    else:
        pickup_space[k] = space_matrix[k][i] + pickup_space[k-1]


# opt_state = list()
# pickup_quality_knapsack = list()
# for i in range(O_v+1):
#     reduce_space = np.array(pickup_space) - i
#     idx = np.argmax(np.where(reduce_space>=0))

#     init_space_array = pickup_space[:idx+1] + [pickup_space[idx] for i in range(clip_number-idx)]
#     init_space_array_2D = np.array([init_space_array for y in range(delta_d+1)])
#     opt_state.append(init_space_array_2D)

#     init_quality_array = pickup_quality[:idx+1] + [(len(pre_d_selected)-1) for i in range(clip_number-idx)]

#     init_quality_array_2D = np.array([init_quality_array for y in range(delta_d+1)])
#     print(init_quality_array)
#     pickup_quality_knapsack.append(init_quality_array_2D)



# opt_state = np.array(opt_state)
# pickup_quality_knapsack = np.array(pickup_quality_knapsack)
# sys.exit()


if __name__=='__main__':
    opt_state = np.zeros((O_v+1, delta_d+1, clip_number+1))
    pickup_quality_knapsack = np.array([[[[7 for i in range(clip_number)] for i in range(clip_number+1)] for j in range(delta_d+1)] for k in range(O_v+1)])


    count = 0
    
    for o in range(1, O_v+1):
        for delta in range(1, delta_d+1):
            for c in range(1, clip_number+1):
                remain_time_array = np.subtract(delta, time_matrix[c-1])
                remain_space_array = np.subtract(o, space_matrix[c-1])
                
                print("remain_time_array", remain_time_array)
                print("remain_space_array",remain_space_array)
                candidatad_quality_arg_time = np.argwhere(remain_time_array>=0).reshape(-1)
                candidatad_quality_arg_space = np.argwhere(remain_space_array>=0).reshape(-1)

                candidatad_quality_arg = np.intersect1d(candidatad_quality_arg_time, candidatad_quality_arg_space)
                print("candidatad_quality_arg", candidatad_quality_arg)
                # if some clip has non zero execution time
                if candidatad_quality_arg.shape[0]>0:
                    remain_time_idx = remain_time_array[candidatad_quality_arg].reshape(-1)
                    remain_space_idx = remain_space_array[candidatad_quality_arg].reshape(-1)
                    print("remain_time_idx", remain_time_idx)
                    print("remain_space_idx",remain_space_idx)
                    
                    # print(profit_matrix[c-1][candidatad_quality_arg])
                    tmp_profit_matrix = list()
                    for q_i in range(len(candidatad_quality_arg)):
                        tmp_profit_matrix.append(opt_state[remain_space_idx[q_i]][remain_time_idx[q_i]][c-1] + profit_matrix[c-1][candidatad_quality_arg[q_i]])
                    

                    tmp_max_idx = np.argmax(np.array(tmp_profit_matrix))
                    tmp_max = tmp_profit_matrix[tmp_max_idx]
  
                else:
                    tmp_max = 0

                print("o:",o," delta:", delta, "c:", c)
                print("Before update:",c, pickup_quality_knapsack[o][delta][c-1])

                if opt_state[o][delta][c-1] >= tmp_max: # not pickup any quality for c
                    opt_state[o][delta][c] = opt_state[o][delta][c-1]
                    pickup_quality_knapsack[o][delta][c] = pickup_quality_knapsack[o][delta][c-1]
                    print("not pickup any quality for c")
                    print("remain state:", pickup_quality_knapsack[o][delta][c])
                    print("quality no change:", opt_state[o][delta][c])
                else: # pick the length of c
                    opt_state[o][delta][c] = tmp_max
                    og_idx = candidatad_quality_arg[tmp_max_idx]
                    pickup_quality_knapsack[o][delta][c] = pickup_quality_knapsack[remain_space_idx[tmp_max_idx]][remain_time_idx[tmp_max_idx]][c-1]
                    print("prev space %d, prev time %d"%(remain_space_idx[tmp_max_idx],remain_time_idx[tmp_max_idx]))
                    print("prev quality", pickup_quality_knapsack[remain_space_idx[tmp_max_idx]][remain_time_idx[tmp_max_idx]][c-1])
                    pickup_quality_knapsack[o][delta][c][c-1] = candidatad_quality_arg[tmp_max_idx]
                    print("update quality", pickup_quality_knapsack[o][delta][c][c-1])
                    count+=1
                print("quality sum", opt_state[o][delta][c])
                print("After update:", c, pickup_quality_knapsack[o][delta][c])
                # if np.sum(pickup_quality_knapsack[o][delta][c])<=40 and count ==10:
                #     sys.exit()
            
                

    pickup_quality = pickup_quality_knapsack[-1][-1][-1].astype(int)
    time_sum = 0
    for key, value in enumerate(pickup_quality):
        time_sum += time_matrix[key][value]

    space_sum = 0
    for key, value in enumerate(pickup_quality):
        space_sum += space_matrix[key][value]

    profit_sum = 0
    for key, value in enumerate(pickup_quality):
        profit_sum += profit_matrix[key][value]

    print("pickup_quality", pickup_quality)
    print("time_sum", time_sum)
    print("space_sum", space_sum)
    print("profit_sum", profit_sum)
    print("profit_sum_opt", opt_state[-1][-1][-1])