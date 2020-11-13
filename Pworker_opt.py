import numpy as np
import pandas as pd
import os
from influxdb import InfluxDBClient
from optimal_downsampling_manager.decision_type import Decision
from optimal_downsampling_manager.resource_predictor.table_estimator import IATable,DownTimeTable,DownRatioTable
from optimal_downsampling_manager.resource_predictor.table_estimator import get_context
import time
import sys
import random
import math



ANALY_LIST=["illegal_parking0","people_counting"]
delta_d = 100#  
# pre_selected={
#     "temporal":[0.75,0.5,0.25], # temporal
#     "bitrate":[1000000.0,500000.0,250000.0], # bitrate
#     "qp":[24.0,30.0,45.0], # QP
#     "None":[-1.0]
# }
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






c_size = []
total_time=0
max_info=0
max_dp=[]
total_space=0
dp_list = []
f_c = 24*60
V = 10

debug = True
downTimeTable = DownTimeTable(False)
downRatioTable = DownRatioTable(False)
iATable = IATable(False)


def brute_force(c, dp, esti_info_amount,cur_dp):
    global c_size, total_time, max_info, max_dp, total_space,V,DOWN_LIST,ANALY_LIST,delta_d,dp_list
    cur_dp.append(dp)
    # get estimated info amount
    for a in range(len(ANALY_LIST)):
        # use the sampling length from problem 1 to estimated the info of no dowmsampling videos
        if dp[0] == 'None':
            if dp[1]==-1.0:
                esti_info_amount += iATable.get_estimation(day_of_week = c[0],time_of_day = c[1] , a_type=ANALY_LIST[a], d_type='None', d_parameter=dp[1], a_parameter=200.0)
            else:
                pass
        # sum of info from downsamled video
        else:
            esti_info_amount += iATable.get_estimation(day_of_week = c[0],time_of_day= c[1], a_type=ANALY_LIST[a], d_type=dp[0], d_parameter=dp[1])

    if id_c==len(clip_list)-1:
        if esti_info_amount > max_info:
            time=0
            space=0
            for c,dp in enumerate(cur_dp):
                if dp[0]=='None':
                    continue
                else:
                    time+=downTimeTable.get_estimation(clip_list[c], d_type = dp[0], d_parameter = dp[1])
            for c,dp in enumerate(cur_dp):
                if dp[0]=='None':
                    if dp[1]==-1.0:
                        space+=c_size[c]
                    else:
                        pass
                else:
                    space+=c_size[c] * downRatioTable.get_estimation(clip_list[c], d_type = dp[0], d_parameter = dp[1])
            if time < delta_d and space < int(V):
                max_info = esti_info_amount
                max_dp = cur_dp
                total_space = space
                total_time = time
        return
    else:
        for dp in dp_list:
            brute_force(id_c+1,dp,esti_info_amount,cur_dp.copy())
        return 




if __name__=='__main__':
    start=time.time()
    DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'video_edge')
    
    result = DBclient.query('SELECT * FROM raw_11_8_down')
    result_point = list(result.get_points(measurement='raw_11_8_down'))
    result_point = random.sample(result_point,k=3)

    total_clips_size = 0.0

    
    clip_array = np.zeros((len(result_point),4),dtype=np.uint8)
    clip_meta = np.zeros((len(result_point),2),dtype=np.float32)
    for id_r, r in enumerate(result_point):
        clip_array[id_r][0], clip_array[id_r][1] = get_context(r['name'])
        clip_array[id_r][2], clip_array[id_r][3] = pre_frame_rate.index(float(r['fps'])), pre_bitrate.index(float(r['bitrate']))
        if r['a_parameter']!=-1:
            clip_meta[id_r][0] = r['a_parameter']
        else:
            clip_meta[id_r][0] = pre_a_selected[0]
        clip_meta[id_r][1] = r['raw_size']
    
    if debug ==True:
        print("Time")
        for k,r in enumerate(clip_array):
            time_dataframe = pd.DataFrame(np.zeros((1,16)),columns=pre_d_selected_)
            print(result_point[k]['name'])
            time_list = []
            for d in range(len(pre_d_selected)):
                time_list.append(downTimeTable.get_estimation(day_of_week=r[0], time_of_day=r[1], p_id=d))

            time_dataframe.iloc[0] = time_list
            print(time_dataframe)

        print("---------------------------------------")
        
        print("Space")
        for k,r in enumerate(clip_array):
            size_dataframe = pd.DataFrame(np.zeros((1,16)),columns=pre_d_selected_)
            size_list = []
            clip_size = clip_meta[k][1]
            print(result_point[k]['name'],clip_size)
            total_clips_size+=clip_size
            size_list = []
            for d in range(len(pre_d_selected)):
                ratio = downRatioTable.get_estimation(day_of_week=r[0], time_of_day=r[1], p_id=d)
                size_list.append(clip_size * ratio)
            size_dataframe.loc[0] = size_list
            print(size_dataframe)
        print("---------------------------------------")

        print("IA")
        for k,r in enumerate(clip_array):
            value_dataframe = pd.DataFrame(np.zeros((1,16)),columns=pre_d_selected_)
            print(result_point[k]['name'])
            value_list = [] 
            for d in range(len(pre_d_selected)):
                ia=0
                for a in range(len(ANALY_LIST)):
                    ia += iATable.get_estimation(day_of_week = r[0], time_of_day = r[1], a_type=a, p_id=d)
                
                value_list.append(ia)
            value_dataframe.loc[0] = value_list
            print(value_dataframe)
    


    target_clip_num = clip_array.shape[0]
    target_deadline = delta_d
    target_space = int(V)

    Estate = np.zeros((target_clip_num,target_space,target_deadline),dtype=np.float32) # Record the info_amount have been preserved
    P_time = np.zeros((target_clip_num,target_space,target_deadline),dtype=np.float32) # Record the time
    P_size = np.zeros((target_clip_num,target_space,target_deadline),dtype=np.float32)

    
    r = np.zeros((target_clip_num),dtype=np.int8)
    # -1 for doing nothing, -2 for deleting 
    r.fill(-2)
    P =  np.array([
            [
                [
                    r for q in range(delta_d) 
                ] for j in range(target_space)
            ] for i in range(target_clip_num)
    ])

    print()

    W = np.ones((target_clip_num,len(ANALY_LIST)),dtype=np.int8)


    print("Consider "+str(target_clip_num)+" clips")

    start_time = time.time()

    for c in range(target_clip_num):
        print(result_point[c]['name'])
        clip_size = clip_meta[c][1]
        last_c=c-1
        # s means at most save that much space
        for s in range(target_space):
            # t means at most that much time
            for t in range(target_deadline): 
                diff_p_comp = list()
                for d in range(1,len(pre_d_selected)):
                    check_fps = int(d/len(pre_frame_rate))
                    check_bitrate = d % len(pre_frame_rate)
                    if check_fps<=clip_array[c][2] and check_bitrate <= clip_array[c][3]:
                            continue
                
                    cost_time = downTimeTable.get_estimation(day_of_week=clip_array[c][0],time_of_day = clip_array[c][1], p_id=d)
                
                    cost_space = clip_size * downRatioTable.get_estimation(day_of_week=clip_array[c][0], time_of_day=clip_array[c][1], p_id=d)

                    reduced_time = int(t-cost_time)
                    reduced_space = int(s-cost_space)

                    if reduced_time<0 or reduced_space<0:
                        continue

                    left_time = t - P_time[last_c][reduced_space][reduced_time] - cost_time
                    left_space = s - P_size[last_c][reduced_space][reduced_time] - cost_space

                    #check if saved space enough and if downsample time meets the t when approach=d and parameter=p
                    if left_time < 0 or left_space < 0:
                        continue
                    
                
                    esti_info_amount=0
                    # get estimated info amount
                    for a in range(len(ANALY_LIST)):
                        # sum of info from downsamled video
                        esti_info_amount += W[c][a] * iATable.get_estimation(day_of_week=clip_array[c][0], time_of_day=clip_array[c][1], a_type=a, p_id=d)*f_c

                    ## weight normalized     
                    # esti_info_amount /= W[c].sum()
                    diff_p_comp.append((esti_info_amount,reduced_space,reduced_time,d,cost_time,cost_space))

                # do nothing 
                raw_reduce_size = int(s-clip_size)
                esti_info_amount = 0
                if raw_reduce_size>0:
                    ## also check information of no downsampling video 
                    if clip_array[c][2]==0 and clip_array[c][3]==0:
                        a_idx = pre_a_selected.index(clip_meta[c][0])
                        for a in range(len(ANALY_LIST)):
                            # use the sampling length from problem 1 to estimated the info of no dowmsampling videos
                            esti_info_amount += W[c][a] * iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, a_parameter=a_idx) * f_c
                    else:  ## do nothing
                        fps = clip_array[c][2]
                        bitrate = clip_array[c][3]
                        p_id = fps * len(pre_frame_rate) + bitrate
                        for a in range(len(ANALY_LIST)):
                            # use the table value from previous problem to estimated the info of dowmsampled videos
                            esti_info_amount += W[c][a] * iATable.get_estimation(day_of_week=clip_array[c][0],time_of_day=clip_array[c][1], a_type=a, p_id=p_id) * f_c
                        diff_p_comp.append((esti_info_amount,raw_reduce_size,t,-1,0,clip_size))


                if len(diff_p_comp) > 0:
                    esti_info_amount,reduced_space,reduced_time,d,cost_time,cost_space = max(diff_p_comp)
                else:
                    continue

                if esti_info_amount + Estate[last_c][reduced_space][reduced_time] > Estate[last_c][s][t]:
                    Estate[c][s][t] = esti_info_amount + Estate[last_c][reduced_space][reduced_time]
                    P[c][s][t] = P[last_c][reduced_space][reduced_time]
                    P[c][s][t][c] = d ## use index instead of value
                    P_time[c][s][t] = P_time[last_c][reduced_space][reduced_time] + cost_time
                    P_size[c][s][t] = P_size[last_c][reduced_space][reduced_time] + cost_space
                else:
                    Estate[c][s][t] = Estate[last_c][s][t]
                    P[c][s][t] = P[last_c][s][t]
                    P_time[c][s][t] = P_time[last_c][s][t]
                    P_size[c][s][t] = P_size[last_c][s][t]

            



    print("Final State: ")
    for k,r in enumerate(P[-1][-1][-1]):    
        if r == -1:
            c_s = clip_meta[r][1]
        elif r==-2:
            c_s = 0
        else:
            c_s = clip_meta[k][1]*downRatioTable.get_estimation(day_of_week=clip_array[k][0], time_of_day=clip_array[k][1], p_id=r)
        print("clip"+str(k),r, c_s)


    # calculate total used space
    estimated_used_size = 0
    for c_id,i in enumerate(P[-1][-1][-1]):
        clip_size = clip_meta[c_id][1]
        if i == -2:
            continue
        elif i == -1:
            estimated_used_size+=clip_size
        else:
            used_space = clip_size * downRatioTable.get_estimation(day_of_week=clip_array[c_id][0], time_of_day=clip_array[c_id][1], p_id=i )
            estimated_used_size += used_space
        
    
    

    print("Total Preserved Info Amount: ",Estate[-1][-1][-1])
    print("Estimated Used Time: ",P_time[-1][-1][-1])
    print("P_size",P_size[-1][-1][-1])
    print("Total Size:", total_clips_size ,"MB. Estimated Used Size:", estimated_used_size ,"MB.")
            
    print("DP total time:",time.time()-start_time,"sec")

    # # print("--------------------------------------------")
    # # print("Brute force answer:")
    # # for id_c, c in enumerate(clip_list):
    # #     c_size.append(round(os.path.getsize(c)/pow(2,20),4))

    # # dp_list = []
    # # for d in pre_selected_brute.keys():
    # #     for p in pre_selected_brute[d]:
    # #         dp_list.append((d,p))
    # # cur_dp=[]
    # # for dp in dp_list:
    # #     brute_force(0,dp,max_info,cur_dp.copy())

    
    # # print("Max info:",max_info)
    # # print("(d,p)",max_dp)
    # # print("Used space:", total_space,"MB")
    # # print("Max time:", total_time,"sec")
