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

delta_i= 1# seconds
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

profit_matrix = np.array([
    [100,27,15,31,4,0],
    [94,45,15,6,32,0],
    [77,45,41,64,22,0],
    [94,54,12,3,45,0],
    [41,21,45,33,21,0],
    [40,26,45,33,21,0]
])

pickup_length = [
    0,0,0,0,0,0
]


def get_time_sum():
    time_sum = 0
    print(pickup_length)
    for key, value in enumerate(pickup_length):
        time_sum += time_matrix[key][value]
    return time_sum
    
if __name__=='__main__':
    while get_time_sum() > delta_i:
        pending_profit_list = list() 
        for key, l in enumerate(pickup_length):

            if time_matrix[key][l] > 0:
                pending_profit_list.append((key, profit_matrix[key][l]/time_matrix[key][l]))

        if len(pending_profit_list)>0: # if there is other length
            victim = min(pending_profit_list, key=lambda x:x[1])
        

        pickup_length[victim[0]] += 1 

