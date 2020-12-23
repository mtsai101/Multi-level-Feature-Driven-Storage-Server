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

delta_i= 250# seconds
clip_number = 5
# pre_a_selected=[4000.0,2000.0,1000.0,500.0,100.0]

pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]

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
    [24,12,10,3,2],
    [32,15,6,4,2],
    [23,14,8,4,1],
    [94,23,15,3,3],
    [35,29,15,6,1],
    [23,17,12,6,2]
])

profit_matrix = np.array([
    [100,27,15,31,4],
    [94,45,15,6,32],
    [77,45,41,64,21],
    [94,54,12,3,45],
    [41,21,45,33,21],
    [40,26,45,33,21]
])
pickup_length = np.array([
    -1,-1,-1,-1,-1,-1
])
if __name__=='__main__':
    opt_state = np.zeros((delta_i, clip_number+1))
    opt_state[:,0] = 0; opt_state[0,:] = 0
    for delta in range(3):
        for c in range(1, clip_number+1):
            print("delta: %d, clip: %d"%(delta, c))
            print(np.subtract(delta, time_matrix[c-1]))
            tmp_time_matrix = np.reshape(
                                np.argwhere(
                                    np.subtract(delta, time_matrix[c-1])>=0
                                    ), -1)

            candicated_profit_list = [opt_state[delta][c-1]]
            
            
            # if some clip has non zero execution time
            if tmp_time_matrix.shape[0]:
                print(profit_matrix[c-1][tmp_time_matrix])
                a=[1,2,3,4]
                print(opt_state[a][c-1].shape)
                tmp_profit_matrix = np.add( opt_state[tmp_time_matrix[:]][c-1] + (profit_matrix[c-1][tmp_time_matrix])[:] )
                candicated_profit_list.extend(tmp_profit_matrix)
            
            
    
            idx = np.argmax(candicated_profit_list)
            opt_state[delta][c] = candicated_profit_list[idx]
            
            if idx == 0:
                pickup_length[c-1] = -1
            else:
                pickup_length[c-1] = time_matrix[c-1][tmp_time_matrix[idx-1]]
            
            
            # break
        # break
