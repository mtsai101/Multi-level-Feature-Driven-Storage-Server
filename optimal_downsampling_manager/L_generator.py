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

ANALY_LIST = ["people_counting","illegal_parking0"]


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
        if L_type=="FIFO" or L_type == "random": 
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
                result = DBclient.query("SELECT * FROM shot_list where \"name\"=\'"+clip['name']+"\'")
                shot_list = ast.literal_eval(list(result.get_points(measurement='shot_list'))[0]['list'])
                if len(shot_list)>0:
                    # decision = Decision(clip_name=clip['name'],a_type='illegal_parking0', a_parameter=1,fps=24.0,bitrate=1000.0, shot_list=shot_list)
                    decision = Decision(clip_name=clip['name'],a_type='people_counting', a_parameter=1,fps=24.0,bitrate=1000.0, shot_list=shot_list)
                    L_list.append(decision)
                else:
                    print("no shot list:", clip)

            return L_list

        elif L_type=='optimal':
            
            Estate = np.zeros((target_clip_num,len(ANALY_LIST),target_deadline),dtype=np.float32)
            L_time = np.zeros((target_clip_num,len(ANALY_LIST),target_deadline),dtype=np.float32)
            r = np.zeros((target_clip_num,len(ANALY_LIST)),dtype=np.int8)
            r.fill(-1)
            L =  np.array([
                    [
                        [
                            r for q in range(delta_i) 
                        ] for j in range(len(ANALY_LIST))
                    ] for i in range(target_clip_num)
                ])
            print()


            W = np.ones((target_clip_num,len(ANALY_LIST)),dtype=float)
            
            print("Consider "+str(target_clip_num)+" clips")

            for c in range(target_clip_num): 
                for a in range(len(ANALY_LIST)):
                    if a==0:
                        if c==0:
                            last_a = 0
                            last_c = 0
                        else:
                            last_a = -1
                            last_c = c-1
                    else:
                        last_c = c
                        last_a = a-1
                    
                    for t in range(target_deadline):
                        diff_p_comp = list()
                        for l in range(len(pre_a_selected)):
                            cost_time = analyTimeTable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=l)

                            reduced_time = int(t-cost_time) 

                            
                            left_time = t - L_time[last_c][last_a][reduced_time] - cost_time

                            if left_time<0:
                                continue  
                            
                            # get estimated info amount 
                            if pre_a_selected[l]!=-1:
                                esti_info_amount = W[c][a] * iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=l) * f_c
                            else:
                                esti_info_amount = 0
                            ## weight normalized
                            # esti_info_amount /= W[c].sum()
                            
                            diff_p_comp.append((esti_info_amount,l,reduced_time,cost_time))

                        if len(diff_p_comp) > 0:
                            esti_info_amount, l, reduced_time, cost_time = max(diff_p_comp)
                        else:
                            continue

                        if esti_info_amount + Estate[last_c][last_a][reduced_time] > Estate[last_c][last_a][t]:
                            Estate[c][a][t] = esti_info_amount + Estate[last_c][last_a][reduced_time]
                            L[c][a][t] = L[last_c][last_a][reduced_time]
                            L[c][a][t][c][a] = l
                            L_time[c][a][t] = L_time[last_c][last_a][reduced_time] + cost_time
                        else:
                            Estate[c][a][t] = Estate[last_c][last_a][t]
                            L[c][a][t] = L[last_c][last_a][t]
                            L_time[c][a][t] = L_time[last_c][last_a][t]

            L_list=[]
            for id_c, c in enumerate(L[-1][-1][-1].tolist()):
                for id_a, l in enumerate(c):
                    if l!=-1:
                        d = Decision(clip_name=clip_list[id_c]['name'], a_type=ANALY_LIST[id_a], a_parameter=pre_a_selected[l])
                        L_list.append(d)
            
            return L_list
                        
        elif L_type=='greedy':
            L =  np.array(np.zeros((target_clip_num,len(ANALY_LIST)),dtype=np.float64))
            L.fill(-1)   
            L_list = []

            W = np.ones((target_clip_num,len(ANALY_LIST)),dtype=float)

            print("Consider "+str(target_clip_num)+" clips")
            total_time=0
            for c in range(target_clip_num):
                for a in range(len(ANALY_LIST)):
                    clip_cost_time = analyTimeTable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=len(pre_a_selected)-1)
                    # Entry of L_list: [info, id_c, id_a, id_l, cost_time]
                    L_list.append([
                            (iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1],a_type=a,a_parameter=len(pre_a_selected)-1)*f_c)/clip_cost_time,
                            c, 
                            a,
                            len(pre_a_selected)-1,
                            clip_cost_time
                        ])
                    total_time += clip_cost_time


            while(total_time > target_deadline):
                tmp = np.argmin(L_list)
                total_time -= L_list[tmp][-1]
                if L_list[tmp][-2] > 0:
                    # decrease the length
                    L_list[tmp][-2] -= 1
                    L_list[tmp][-1] = analyTimeTable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=L_list[tmp][2], a_parameter=L_list[tmp][-2])

                    L_list[tmp][0] = (iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1],a_type=L_list[tmp][2],a_parameter=L_list[tmp][-2])*f_c)/ L_list[tmp][-1]
                    total_time += L_list[tmp][-1]
                else:    
                    del L_list[tmp]

            total_info_amount = 0

            tmp=[]
            for el in L_list:
                if el[3]!=-1:
                    d = Decision(clip_name=clip_list[el[1]]['name'], a_type=ANALY_LIST[el[2]],a_parameter=pre_a_selected[el[3]])
                    tmp.append(d)
                    # total_info_amount += iATable.get_estimation(clip_list[el[1]], a_type=ANALY_LIST[el[2]], a_parameter=el[3])*el[3]
            L_list = tmp

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





    



 
    
