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

pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]

delta_i = 300 # second
f_c = 24 * 60 # fps * second
week=[
    [0 for i in range(12)],
    [0 for i in range(12)]
]
# analyTimeTable = AnalyTimeTable(False)
# iATable = IATable(False)
# this generator will conduct information amount estimation algorithm and generate L
def generate_L(L_type='',clip_list=[], process_num=1):

    clip_array = np.zeros((len(clip_list),2),dtype=np.uint8)


    for id_r, r in enumerate(clip_list):
        clip_array[id_r][0], clip_array[id_r][1] = get_context(r['name'])

    target_clip_num = clip_array.shape[0]
    target_deadline = delta_i


    print("Start to generating L...")
    try:
        if L_type=="FIFO": 
            L_list=list()
            """
                The followings are for build the prediction models
            """
        
            # for i in clip_list:
            #     for a in ANALY_LIST:
            #         decision = Decision(clip_name=i['name'],a_type=a,a_parameter=-1.0,fps=24.0,bitrate=1000.0)
            #         L_list.append(decision)
            DBclient = InfluxDBClient('localhost', data['global']['database'], 'root', 'root', 'storage')
            for clip in clip_list:
                # decision = Decision(clip_name=clip['name'],a_type='illegal_parking0', a_parameter=1,fps=24.0,bitrate=1000.0, shot_list=shot_list)
                decision = Decision(clip_name=clip['name'],
                            a_type=clip['a_type'], 
                            a_parameter=int(clip['a_parameter']),
                            prev_fps=int(clip['prev_fps']), 
                            prev_bitrate=int(clip['bitrate']), 
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
    

def get_ia_time_from_matrix(L,W):
    min_c = -1
    min_a = -1
    min_info = -1
    cost_time = 0 
    for dk,dv in L.iteritems():
        for sk,sv in dv.iteritems():
            tmp = W[dk][sk] * IATable().get_estimation(dk,a_type=ANALY_LIST[sk],parameter=sv)
            if min_info > tmp:
                min_info = tmp
                min_c = dk
                min_a = sk
            cost_time += AnalyTimeTable().get_estimation(dk,a_type=ANALY_LIST[sk],parameter=sv) * sv
    return cost_time,min_c,min_a





    



 
    
