import numpy as np
import pandas as pd
import threading
import os
from pathlib import Path
from threading import Lock
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision
# from optimal_downsampling_manager.resource_predictor.table_estimator import IATable,DownTimeTable,DownRatioTable,get_context
from influxdb import InfluxDBClient
import time
import sys
import random
import numpy
import math
import csv
import yaml

ANALY_LIST=["illegal_parking0","people_counting"]
delta_d = 3600*6 
pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]
pre_frame_rate = [24.0,12.0,6.0,1.0]
pre_bitrate = [1000.0, 500.0, 100.0, 10.0]
pre_d_selected = []
pre_d_selected_=[]
for f in pre_frame_rate:
    for b in pre_bitrate:
        pre_d_selected.append([f,b])
        pre_d_selected_.append((f,b))
pre_d_selected=np.array(pre_d_selected) 

debug = True
f_c = 24*60
V = 3*1024
# iATable = IATable(False)
# downTimeTable = DownTimeTable(False)
# downRatioTable = DownRatioTable(False)
week=[
    [0 for i in range(12)],
    [0 for i in range(12)]
]

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database'], 'root', 'root', 'storage')

# this generator will conduct information amount estimation algorithm and generate S
def generate_P(P_type='', clip_list=[]):

    # target_clip_num = clip_array.shape[0]
    # target_deadline = delta_d
    # target_space = int(V)

    space_experiment = Path("./storage_server_volume/storage_video")

    if P_type=='None':
        P_list=[]
        
        for c in clip_list:
            d = Decision(
                        clip_name=c['name'], 
                        prev_fps = int(c['prev_fps']), 
                        prev_bitrate = int(c['prev_bitrate']),
                        fps=int(c['fps']),
                        bitrate=int(c['bitrate']),
                        others=[int(c['a_para_illegal_parking']),int(c['a_para_people_counting']),float(c['raw_size'])]
                )
            P_list.append(d)

        return P_list

    elif P_type=='FIFO': 
        P_list=[]
        space_experiment = Path("./storage_server_volume/storage_video")
        sumsize = sum(f.stat().st_size for f in space_experiment.glob('**/*') if f.is_file())
        sumsize = sumsize/pow(2,20)
        clip_count=0
        while sumsize > V:
            victim_clip_size = os.path.getsize(clip_list[clip_count]['name'])/pow(2,20)
            sumsize -= victim_clip_size
            d = Decision(clip_name=clip_list[clip_count]['name'], fps=-1.0, bitrate=-1.0, others=[clip_list[clip_count]['a_para_illegal_parking'],clip_list[clip_count]['a_para_people_counting'],clip_list[clip_count]['size']])
            P_list.append(d)
            clip_count+=1
        print("FIFO get out")
        preserved_ia = 0

        return P_list

    elif P_type=='greedy':
        start_time = time.time()
        P =  np.array(np.zeros((target_clip_num),dtype=np.int8))
        ## -1 for deleting; -2 for do nothing
        P.fill(-2)
        W = np.ones((target_clip_num,len(ANALY_LIST)),dtype=np.float16)

        print("Consider "+str(target_clip_num)+" clips")
        total_esti_time = 0
        total_esti_space = 0
        total_esti_info_amount = 0
        info_list_all = []

        for c in range(target_clip_num):
            clip_cur_size = os.path.getsize(clip_list[c]['name'])/pow(2,20)
            total_esti_space += clip_cur_size
            ia_amount = 0
            for a in range(len(ANALY_LIST)):
                a_idx = pre_a_selected.index(clip_meta[c][a])
                ia_amount += iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter = a_idx) * f_c 
            downsample_time = 0
            fps = clip_array[c][2]
            bitrate = clip_array[c][3]
            ## order: larger info, latest clip, smaller size
            info_list_all.append([c, ia_amount, clip_cur_size, downsample_time, fps, bitrate])

        while(total_esti_space>target_space or total_esti_time>target_deadline):
            
            info_list_all = sorted(info_list_all,key= lambda x: x[1]/x[2],reverse=True)
            # print(info_list_all[-2],info_list_all[-1])
            vicitm_clip = info_list_all[-1][0]

            victim_clip_size = info_list_all[-1][2]
            vicitm_clip_fps = info_list_all[-1][4]
            vicitm_clip_bitrate = info_list_all[-1][5]

            next_d = -1 # -1 for deleting
            trade_size = victim_clip_size
            next_size = 0
            next_downsample_time = 0
            for d in range(1,len(pre_d_selected)):
                check_fps = int(d/len(pre_frame_rate))
                check_bitrate = d % len(pre_frame_rate)
                if check_fps<=vicitm_clip_fps and check_bitrate <= vicitm_clip_bitrate:
                        continue
                tmp_size = clip_meta[vicitm_clip][2] * downRatioTable.get_estimation(day_of_week=clip_array[vicitm_clip][0],time_of_day=clip_array[vicitm_clip][1],p_id=d)
                tmp_trade_size = victim_clip_size - tmp_size

                if tmp_trade_size > 0 and tmp_trade_size < trade_size:
                    trade_size = tmp_trade_size
                    next_d = d
                    next_size = tmp_size

                    next_downsample_time  = downTimeTable.get_estimation(day_of_week=clip_array[vicitm_clip][0],time_of_day=clip_array[vicitm_clip][1],p_id=next_d)

            total_esti_space = total_esti_space - trade_size
            total_esti_time = total_esti_time - info_list_all[-1][3] + next_downsample_time

            if next_d == -1:
                P[info_list_all[-1][0]] = -1
                info_list_all.pop()
                continue
            else:
                info_list_all[-1]=[ vicitm_clip, info_list_all[-1][1], next_size, next_downsample_time, int(next_d/len(pre_frame_rate)), next_d%len(pre_frame_rate)]

        for el in info_list_all:
            if el[4]==clip_array[id_r][2] and clip_array[id_r][3] == el[5]:
                P[el[0]] = -2  
            else:
                P[el[0]] = el[4] * len(pre_frame_rate) + el[5] ## save the video clip with raw
        
        # record_P_result(clip_array,P,total_esti_space)
        P_list=[]

        for c_id,p_id in enumerate(P):
            if p_id==-2 or p_id==0: # do nothing
                continue

            elif p_id==-1: # remove the clip from the server
                d = Decision(clip_name=clip_list[c_id]['name'], fps=-1.0, bitrate=-1.0, others=[clip_list[c_id]['a_parameter_0'],clip_list[c_id]['a_parameter_1'],clip_list[c_id]['raw_size']])
                P_list.append(d)

            else:
                fps = pre_frame_rate[int(p_id/len(pre_frame_rate))]
                bitrate = pre_bitrate[p_id%len(pre_frame_rate)] 
                d = Decision(clip_name=clip_list[c_id]['name'], fps=fps, bitrate=bitrate, others=[clip_list[c_id]['a_parameter_0'],clip_list[c_id]['a_parameter_1'],clip_list[c_id]['raw_size']])
                P_list.append(d)
        return P_list

    elif P_type == 'EF': # save to 100kbps or delete it
        P_list=[]
        space_experiment = Path("/home/min/ssd/space_experiment")
        sumsize = sum(f.stat().st_size for f in space_experiment.glob('**/*') if f.is_file())
        sumsize = sumsize/pow(2,20)
        for id_c, c in enumerate(clip_list):
            if clip_array[id_c][3]!=2:
                fps = pre_frame_rate[clip_array[id_c][2]]
                d = Decision(clip_name=c['name'], fps=fps, bitrate=100.0, others=[c['a_parameter_0'],c['a_parameter_1'],c['raw_size']])
                P_list.append(d)
                sumsize -= clip_meta[id_c][2] * (1-downRatioTable.get_estimation(day_of_week=clip_array[id_c][0],time_of_day=clip_array[id_c][1], p_id=2))
            if sumsize < V:
                break

        clip_count=0
        while sumsize > V:
            victim_clip_size = os.path.getsize(clip_list[clip_count]['name'])/pow(2,20)
            sumsize -= victim_clip_size
            d = Decision(clip_name=clip_list[clip_count]['name'], fps=-1, bitrate=-1, others=[c['a_parameter_0'],c['a_parameter_1'],c['raw_size']])
            P_list.append(d)
            clip_count+=1
        
        print("EF get out!")      
        return P_list

    elif P_type == 'EFR': # save to 6 fps or delte it
        P_list=[]
        space_experiment = Path("/home/min/ssd/space_experiment")
        sumsize = sum(f.stat().st_size for f in space_experiment.glob('**/*') if f.is_file())
        sumsize = sumsize/pow(2,20)
        for id_c, c in enumerate(clip_list):
            if clip_array[id_c][2]!=2:
                bitrate = pre_bitrate[clip_array[id_c][3]]
                d = Decision(clip_name=c['name'], fps=6.0, bitrate=bitrate, others=[c['a_parameter_0'],c['a_parameter_1'],c['raw_size']])
                P_list.append(d)
                sumsize -= clip_meta[id_c][2] * (1-downRatioTable.get_estimation(day_of_week=clip_array[id_c][0],time_of_day=clip_array[id_c][1], p_id=10))

        clip_count=0
        while sumsize > V:
            victim_clip_size = os.path.getsize(clip_list[clip_count]['name'])/pow(2,20)
            sumsize -= victim_clip_size
            d = Decision(clip_name=clip_list[clip_count]['name'], fps=-1, bitrate=-1, others=[c['a_parameter_0'],c['a_parameter_1'],c['raw_size']])
            P_list.append(d)
            clip_count+=1

        print("EFR get out!")      
        return P_list
    else:
        print('Unknow P_type')



# def record_P_result(clip_array,P,total_esti_space):
#     ## record estimated information
#     total_esti_info_amount = 0
#     for c,d in enumerate(P):
        
#         if d==-2:
#             p_id = clip_array[c][2] * len(pre_frame_rate) + clip_array[c][3]
#             for a in range(len(ANALY_LIST)):
#                 total_esti_info_amount += iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, p_id=p_id)*f_c
#         elif d==-1:
#             continue
#         elif d==0:
#             a_idx = pre_a_selected.index(clip_meta[id_r][0])
#             for a in range(len(ANALY_LIST)):
#                 total_esti_info_amount += iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=a_idx)*f_c
#         else:
#             for a in range(len(ANALY_LIST)):
#                 total_esti_info_amount += iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, p_id=d)*f_c
#         total_esti_info_amount

#     if not os.path.isdir("./prob2"):
#         os.system("mkdir prob2")
#     with open("./prob2/estimate_info.csv", 'a', newline='') as f:
#         writer = csv.writer(f)
#         writer.writerow(['esti_info','esti_space'])
#         writer.writerow([total_esti_info_amount,total_esti_space])
        

    # elif P_type=='optimal':
    #     Estate = np.zeros((target_clip_num,target_space,target_deadline),dtype=np.float32) # Record the info_amount have been preserved
    #     P_time = np.zeros((target_clip_num,target_space,target_deadline),dtype=np.float32) # Record the time
    #     P_size = np.zeros((target_clip_num,target_space,target_deadline),dtype=np.float32)

    #     r = np.zeros((target_clip_num),dtype=np.int8)
    #     # -1 for doing nothing, -2 for deleting 
    #     r.fill(-2)
    #     P =  np.array([
    #             [
    #                 [
    #                     r for q in range(delta_d) 
    #                 ] for j in range(target_space)
    #             ] for i in range(target_clip_num)
    #     ])

    #     print()

    #     W = np.ones((target_clip_num,len(ANALY_LIST)),dtype=np.int8)


    #     print("Consider "+str(target_clip_num)+" clips")

    #     start_time = time.time()

    #     for c in range(target_clip_num):
    #         clip_size = clip_meta[c][2]
    #         last_c=c-1
    #         # s means at most save that much space
    #         for s in range(target_space):
    #             # t means at most that much time
    #             for t in range(target_deadline): 
    #                 diff_p_comp = list()
    #                 for d in range(1,len(pre_d_selected)):
    #                     check_fps = int(d/len(pre_frame_rate))
    #                     check_bitrate = d % len(pre_frame_rate)
    #                     if check_fps<=clip_array[c][2] and check_bitrate <= clip_array[c][3]:
    #                             continue
                    
    #                     cost_time = downTimeTable.get_estimation(day_of_week=clip_array[c][0],time_of_day = clip_array[c][1], p_id=d)
                    
    #                     cost_space = clip_size * downRatioTable.get_estimation(day_of_week=clip_array[c][0], time_of_day=clip_array[c][1], p_id=d)

    #                     reduced_time = int(t-cost_time)
    #                     reduced_space = int(s-cost_space)

    #                     if reduced_time<0 or reduced_space<0:
    #                         continue

    #                     left_time = t - P_time[last_c][reduced_space][reduced_time] - cost_time
    #                     left_space = s - P_size[last_c][reduced_space][reduced_time] - cost_space

    #                     #check if saved space enough and if downsample time meets the t when approach=d and parameter=p
    #                     if left_time < 0 or left_space < 0:
    #                         continue
                        
                    
    #                     esti_info_amount=0
    #                     # get estimated info amount
    #                     for a in range(len(ANALY_LIST)):
    #                         # sum of info from downsamled video
    #                         esti_info_amount += W[c][a] * iATable.get_estimation(day_of_week=clip_array[c][0], time_of_day=clip_array[c][1], a_type=a, p_id=d)*f_c

    #                     ## weight normalized     
    #                     # esti_info_amount /= W[c].sum()
    #                     diff_p_comp.append((esti_info_amount,reduced_space,reduced_time,d,cost_time,cost_space))

    #                 # do nothing 
    #                 raw_reduce_size = int(s-clip_size)
    #                 esti_info_amount = 0
    #                 if raw_reduce_size>0:
    #                     ## also check information of no downsampling video 
    #                     if clip_array[c][2]==0 and clip_array[c][3]==0:
    #                         for a in range(len(ANALY_LIST)):
    #                             # use the sampling length from problem 1 to estimated the info of no dowmsampling videos
    #                             a_idx = pre_a_selected.index(clip_meta[c][a])
    #                             esti_info_amount += W[c][a] * iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=a_idx) * f_c
    #                     else:  ## do nothing
    #                         fps = clip_array[c][2]
    #                         bitrate = clip_array[c][3]
    #                         p_id = fps * len(pre_frame_rate) + bitrate
    #                         for a in range(len(ANALY_LIST)):
    #                             # use the table value from previous problem to estimated the info of dowmsampled videos
    #                             esti_info_amount += W[c][a] * iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, p_id=p_id) * f_c
    #                         diff_p_comp.append((esti_info_amount,raw_reduce_size,t,-1,0,clip_size))


    #                 if len(diff_p_comp) > 0:
    #                     esti_info_amount,reduced_space,reduced_time,d,cost_time,cost_space = max(diff_p_comp)
    #                 else:
    #                     continue

    #                 if esti_info_amount + Estate[last_c][reduced_space][reduced_time] > Estate[last_c][s][t]:
    #                     Estate[c][s][t] = esti_info_amount + Estate[last_c][reduced_space][reduced_time]
    #                     P[c][s][t] = P[last_c][reduced_space][reduced_time]
    #                     P[c][s][t][c] = d ## use index instead of value
    #                     P_time[c][s][t] = P_time[last_c][reduced_space][reduced_time] + cost_time
    #                     P_size[c][s][t] = P_size[last_c][reduced_space][reduced_time] + cost_space
    #                 else:
    #                     Estate[c][s][t] = Estate[last_c][s][t]
    #                     P[c][s][t] = P[last_c][s][t]
    #                     P_time[c][s][t] = P_time[last_c][s][t]
    #                     P_size[c][s][t] = P_size[last_c][s][t]


    #     P_list = []
    #     for c_id,d in enumerate(P[-1][-1][-1]):
    #         if d == -2: # deleting 
    #             d = Decision(clip_name=clip_list[c_id]['name'], fps=-1.0, bitrate=-1.0, others=[clip_list[c_id]['a_parameter_0'],clip_list[c_id]['a_parameter_1'],clip_list[c_id]['raw_size']])
    #             P_list.append(d)
    #             continue
    #         elif d == -1 or 0: # do nothing
    #             continue
    #         else:
    #             fps = int(d/len(pre_frame_rate))
    #             bitrate = d % len(pre_d_selected) 
    #             d = Decision(clip_name=clip_list[c_id]['name'], fps=fps, bitrate=bitrate,a_parameter=clip_list[c_id]['a_parameter'],others=clip_list[c_id]['raw_size'])
    #         P_list.append(d)

    #     return P_list




    



 
    
