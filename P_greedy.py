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

O_v = 500# MB
delta_d = 50
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
    [24,12,10,3,2,0],
    [32,15,6,4,2,0],
    [23,14,8,4,1,0],
    [94,23,15,3,3,0],
    [35,29,15,6,1,0],
    [23,17,12,6,2,0]
])

space_matrix = np.array([
    [200,100,45,23,21,0],
    [120,100,36,27,17,0],
    [250,74,54,12,8,0],
    [320,150,88,34,14,0],
    [250,74,28,15,14,0],
    [100,45,36,17,12,0]
])

profit_matrix = np.array([
    [5,4,4,3,2,1,0],
    [94,45,15,6,32,0],
    [77,45,41,64,22,0],
    [94,54,12,45,19,0],
    [41,21,45,33,21,0],
    [45,26,33,21,0]
])

pickup_quality = [
    0,0,0,0,0,0
]

pickup_info = np.array([
    0,0,0,0,0,0
])

def get_time_sum():
    time_sum = 0
    print(pickup_length)
    for key, value in enumerate(pickup_length):
        time_sum += time_matrix[key][value]
    return time_sum

space_sum  = np.sum(space_matrix, axis=0)[0]
time_sum = 0

# space_sum = space_sum - space_matrix[c][pickup_quality[c]] + space_matrix[c][d] 
# time_sum = time_sum - time_matrix[c][pickup_quality[c]] + time_matrix[c][d]


if __name__=='__main__':
    profit_list = []
    for c, q in enumerate(pickup_quality):
        s = space_matrix[c][q]

        if s>0 and pickup_quality[c] < space_matrix.shape[1]:
            profit_list.append((c, profit_matrix[c][q]/s)) 
 

    while space_sum > O_v or time_sum > delta_d:
        
        
        victim_c = min(profit_list, key= lambda x: x[1])
        print(profit_list)


        c = victim_c[0]
        profit_list.remove(victim_c)
        d = pickup_quality[c] + 1
        
        
        if pickup_quality[c]==space_matrix.shape[1]-1:
            print("the victim can not be downsample anymore")
            continue

        
        space_sum = space_sum - space_matrix[c][pickup_quality[c]] + space_matrix[c][d] 
        
        time_sum = 0
        for t_key,v in enumerate(pickup_quality):
            time_sum += time_matrix[t_key][v]


        s = space_matrix[c][d]
        if s > 0: ## make sure the last one ios zero
            profit_list.append((c, profit_matrix[c][d]/s)) 

        pickup_quality[c] = d
        print(c, pickup_quality[c])
        if len(profit_list) == 0:
            print("np video")
            break
        print(pickup_quality)
       
        print(space_sum, time_sum)
    print("Final :", pickup_quality)