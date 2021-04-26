import numpy as np
import random
import datetime
import sys
import pandas as pd
from influxdb import InfluxDBClient
from collections import OrderedDict 
from .decision_type import Decision
# from .resource_predictor.table_estimator import IATable, AnalyTimeTable

import random
from optimal_downsampling_manager.resource_predictor.table_estimator import get_context
import os

import ast
import yaml
ANALY_LIST = ["people_counting","illegal_parking0"]

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

# this generator will conduct information amount estimation algorithm and generate L
def generate_L(L_type='',clip_list=[]):

    # weight assignment of video could be written here
    # clip_array = np.zeros((len(clip_list),2),dtype=np.uint8)
    # for id_r, r in enumerate(clip_list):
    #     clip_array[id_r][0], clip_array[id_r][1] = get_context(r['name'])

    # target_clip_num = clip_array.shape[0]


    print("Start to generating L...")
    try:
        if L_type=="EXP":
            """
                Experimental usage
            """

            L_list = list()

            for clip in clip_list:
                decision = Decision(clip_name=clip['name'],
                            a_type = clip['a_type'], 
                            a_parameter=int(clip['a_parameter']),
                            fps= int(clip['fps']), 
                            bitrate=int(clip['bitrate']))
                L_list.append(decision)
            return L_list

        elif L_type=='optimal':
            
            return L_list
                        
        elif L_type=='heuristic':
            
            return L_list

        else:
            print('Unknow L_type')

    except Exception as e:
        print(e)
    





    



 
    
