import numpy as np
import pandas as pd
import threading
import os
from threading import Lock
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision

import time
import sys
import random

ANALY_LIST=["illegal_parking0","people_counting"]

delta_i= 10# seconds
clip_number = 5
# pre_a_selected=[4000.0,2000.0,1000.0,500.0,100.0]


pre_a_selected=[1,24,48,96,144,0]
pre_a_selected_tuple = []
for a in pre_a_selected:
    for b in pre_a_selected:
        pre_a_selected_tuple.append((a,b)) 


valid_list=[]
f_c = 24 * 60 # fps * second
# analyTimeTable = AnalyTimeTable(False)
# iATable = IATable(False)
# debug = False
trigger_interval=6

clock = {
            "month":11,
            "day":8,
            "hour":6,
            "min_":00
        }
time_matrix = np.array([
    [24,10,3,12,2,0],
    [32,15,6,4,2,0],
    [23,14,8,4,1,0],
    [94,23,15,3,3,0],
    [35,29,15,6,1,0],
    [23,17,12,6,2,0]
])

profit_matrix = np.array([
    [100,27,15,31,4,0],
    [94,45,15,6,32,0],
    [77,45,41,64,21,0],
    [94,54,12,3,45,0],
    [41,21,45,33,21,0],
    [40,26,45,33,21,0]
])
pickup_length = np.array([
    0,0,0,0,0
])
pickup_length_knapsack = np.zeros(delta_i, clip_number)

if __name__=='__main__':
    opt_state = np.zeros((clip_number+1, delta_i+1))
    opt_state[:,0] = 0; opt_state[0,:] = 0
    for delta in range(7,15):
        for c in range(1, clip_number+1):
            print("delta: %d, clip: %d"%(delta, c))

            remain_time_array = np.subtract(delta, time_matrix[c-1])
            candidatad_length_arg = np.argwhere(remain_time_array>=0)
            print("remain_time_array", remain_time_array)
            print("candidatad_length_arg", candidatad_length_arg)
            # if some clip has non zero execution time
            if candidatad_length_arg.shape[0]>0:
                print("remain_time_array[candidatad_length_arg]", remain_time_array[candidatad_length_arg].reshape(-1))
                remain_time_idx = remain_time_array[candidatad_length_arg].reshape(-1)

                print("profit matrix", profit_matrix[3][candidatad_length_arg].reshape(-1))
                print("opt_state[c-1][remain_time_idx]", opt_state[c-1][remain_time_idx])
                print(opt_state[c-1][remain_time_idx] + profit_matrix[3][candidatad_length_arg].reshape(-1))
                
                tmp_profit_matrix = opt_state[c-1][remain_time_idx] + profit_matrix[c-1][candidatad_length_arg].reshape(-1)
                tmp_max_idx = np.argmax(tmp_profit_matrix)
                tmp_max = tmp_profit_matrix[tmp_max_idx]
                print(tmp_max_idx, tmp_max)
                sys.exit()
            else:
                tmp_max = 0
    
            if opt_state[c-1][delta] >= tmp_max: # not pickup any length of c
                opt_state[c][delta] = opt_state[c-1][delta]
                pickup_length_knapsack[delta][c-1] = len(pre_a_selected_tuple)-1
            else: # pick the length of c
                opt_state[c][delta] = tmp_max
                pickup_length_knapsack[delta] = pickup_length_knapsack[remain_time_idx][:c-1]
                pickup_length_knapsack[delta][c-1] = candidatad_length_arg[tmp_max_idx]
            
            
            # break
        # break
