import numpy as np
import pandas as pd
import threading
import os
from threading import Lock
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision
from optimal_downsampling_manager.resource_predictor.table_estimator import IATable,DownTimeTable,DownRatioTable,get_context
import csv
import time
import sys
import numpy
import math

ANALY_LIST=["illegal_parking0","people_counting"]
delta_d=3600*6 # 1 minutes
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

debug = False
f_c = 24*60
V = 3000
iATable = IATable(False)
downTimeTable = DownTimeTable(False)
downRatioTable = DownRatioTable(False)
DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')

def P_greedy():
    clip_list =[]
    result_list = []
    
    # for i in range(9,11):
    #     result = DBclient.query("SELECT * FROM raw_11_"+str(i))
    #     result_list = list(result.get_points(measurement="raw_11_"+str(i)))
    #     for r in result_list:
    #         da,ti = get_context(r['name'])
    #         if ti==11:
    #             continue
    #         ar = DBclient.query("SELECT * FROM analy_result_greedy WHERE \"name\"=\'"+r['name']+"\'")
    #         ar_list = list(ar.get_points(measurement="analy_result_greedy"))

    #         a_parameter_ill = 5.0
    #         a_parameter_peo = 5.0

    #         for ap in ar_list:
    #             if ap['a_type']=='illegal_parking0':
    #                 a_parameter_ill = ap['a_parameter']
    #             elif ap['a_type']=='people_counting':
    #                 a_parameter_peo = ap['a_parameter']

    #         clip_list.append({
    #             'name':r['name'],
    #             'a_parameter_0':a_parameter_ill,
    #             'a_parameter_1':a_parameter_peo,
    #             'raw_size':6,
    #             'fps':24.0,
    #             'bitrate':1000.0
    #         })

    with open('./prob2_greedy/clips_13_12.csv') as f:
        rows = csv.reader(f)
        result_list=[]
        clip_list=[]
        for row in rows:
            result_list.append({
                'name':row[0],
                'fps':row[1],
                'bitrate':row[2]
            })


    for r in result_list:
        da,ti = get_context(r['name'])
        if ti==11:
            continue
        ar = DBclient.query("SELECT * FROM analy_result_greedy WHERE \"name\"=\'"+r['name']+"\'")
        ar_list = list(ar.get_points(measurement="analy_result_greedy"))

        a_parameter_ill = 5.0
        a_parameter_peo = 5.0


        for ap in ar_list:
            if ap['a_type']=='illegal_parking0':
                a_parameter_ill = ap['a_parameter']
            elif ap['a_type']=='people_counting':
                a_parameter_peo = ap['a_parameter']
        
        tmp_c = r['name'].split("/")
        d = pre_frame_rate.index(float(r['fps'])) * len(pre_frame_rate) + pre_bitrate.index(float(r['bitrate']))
        cur_size = os.path.getsize('/home/min/ssd/'+'/'+tmp_c[-2]+'/'+tmp_c[-1])/pow(2,20) * downRatioTable.get_estimation(day_of_week=da, time_of_day=ti, p_id=d)

        clip_list.append({
            'name':r['name'],
            'a_parameter_0':a_parameter_ill,
            'a_parameter_1':a_parameter_peo,
            'raw_size':6,
            'fps':r['fps'],
            'bitrate':r['bitrate'],
            'cur_size':cur_size
        })
    

    total_clips_size = 0.0
    
    
    clip_array = np.zeros((len(clip_list),4),dtype=np.uint8)
    clip_meta = np.zeros((len(clip_list),3),dtype=np.float32)
    for id_r, r in enumerate(clip_list):
        clip_array[id_r][0], clip_array[id_r][1] = get_context(r['name'])
        clip_array[id_r][2], clip_array[id_r][3] = pre_frame_rate.index(float(r['fps'])), pre_bitrate.index(float(r['bitrate']))
        
        clip_meta[id_r][0] = r['a_parameter_0']
        clip_meta[id_r][1] = r['a_parameter_1']

        clip_meta[id_r][2] = r['raw_size']
    
    if debug ==True:
        print("Time")
        for k,r in enumerate(clip_array):
            time_dataframe = pd.DataFrame(np.zeros((1,16)),columns=pre_d_selected_)
            # print(result_point[k]['name'])
            time_list = []
            for d in range(len(pre_d_selected)):
                time_list.append(downTimeTable.get_estimation(day_of_week=r[0], time_of_day=r[1], p_id=d))

            time_dataframe.iloc[0] = time_list
            # print(time_dataframe)

        print("---------------------------------------")
        
        print("Space")
        for k,r in enumerate(clip_array):
            size_dataframe = pd.DataFrame(np.zeros((1,16)),columns=pre_d_selected_)
            size_list = []
            clip_size = clip_meta[k][2]
            # print(result_point[k]['name'],clip_size)
            total_clips_size+=clip_size
            size_list = []
            for d in range(len(pre_d_selected)):
                ratio = downRatioTable.get_estimation(day_of_week=r[0], time_of_day=r[1], p_id=d)
                size_list.append(clip_size * ratio)
            size_dataframe.loc[0] = size_list
            # print(size_dataframe)
        print("---------------------------------------")

        print("IA")
        for k,r in enumerate(clip_array):
            value_dataframe = pd.DataFrame(np.zeros((1,16)),columns=pre_d_selected_)
            # print(result_point[k]['name'])
            value_list = [] 
            for d in range(len(pre_d_selected)):
                ia=0
                for a in range(len(ANALY_LIST)):
                    if d==0:
                        a_idx = pre_a_selected.index(clip_meta[id_r][a])
                        ia += iATable.get_estimation(day_of_week = r[0], time_of_day = r[1], a_type=a, a_parameter=a_idx)
                    else:
                        ia += iATable.get_estimation(day_of_week = r[0], time_of_day = r[1], a_type=a, p_id=d)
                
                value_list.append(ia)
            value_dataframe.loc[0] = value_list
            # print(value_dataframe)


    start_time =time.time()

    target_clip_num = clip_array.shape[0]
    target_deadline = delta_d
    target_space = int(V) ## how much storage space can use



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
        clip_cur_size = clip_list[c]['cur_size']
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

    tmp_time=time.time()
    count=0
    while(total_esti_space>target_space or total_esti_time>target_deadline):
        count+=1
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

    end_time=time.time()

    with open('./Downsampling_running_time/Downsampling_running_time_13.csv','w',newline='') as f:
        writer = csv.writer(f)
        writer.writerow([total_esti_time])
     
    return P_list

    # print("Final State: ")
    # for k,r in enumerate(P):    
    #     print("clip"+str(k),r)

    # print("Total Preserverd Info Amount:",total_esti_info_amount)
    # print("Total size",total_clips_size,"MB")
    # print("Total estimated space:",total_esti_space,"MB")
    # print("Total estimation downsampling time:", total_esti_time)
    # print("Algorithm running time:",time.time()-start_time)

if __name__=='__main__':
    P_list = P_greedy()
    # for p in P_list:
    #     print(p.clip_name,p.fps,p.bitrate)
    
        
    