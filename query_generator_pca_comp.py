from influxdb import InfluxDBClient
from numpy.random import default_rng
import numpy as np
import pandas as pd
import datetime
import random
import csv 
import os
from optimal_downsampling_manager.resource_predictor.estimate_table import Degraded_IATable, get_context, DownTimeTable, DownRatioTable, Degraded_Q_IATable, get_month_and_day
from math import e
import sys
import yaml
import argparse
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', data['global']['database_name'])
resultDBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', 'exp_storage')

result = DBclient.query('SELECT * FROM MaxAnalyticTargetNumber')
MaxTargetTable = pd.DataFrame(list(result.get_points(measurement="MaxAnalyticTargetNumber")))

result = DBclient.query('SELECT * FROM visual_features_entropies_PCA_normalized')
PCATable = pd.DataFrame(list(result.get_points(measurement="visual_features_entropies_PCA_normalized")))


# alog_list = ['EF','EFR','FIFO','heuristic','opt', 'approx']
alog_list = ['heuristic']

SEEN_ANALY_LIST = ["illegal_parking0", "people_counting"]
UNSEEN_ANALY_LIST = ["illegal_parking1", "car_counting"]

if __name__=='__main__':

    start_day = 14
    end_day = 15
    size  = (end_day-start_day+1)
    poisson_rate = np.random.poisson(8,size) # 10 request / 24 hour 

    query_video_list = []

    rng = default_rng()
    full_length_sample_quality_info_df = None
    full_info_df = None
    for r in range(start_day,end_day+1):
        # if r==12 or r==11:
            # continue
        result = DBclient.query("SELECT * FROM raw_11_"+str(r))
        all_day_video = list(result.get_points(measurement="raw_11_"+str(r)))
        chosen_list = rng.choice(len(all_day_video), poisson_rate[r-start_day],replace=True)
        chosen_list = sorted(chosen_list)
        for c in chosen_list:
            query_video_list.append(all_day_video[c])

        result = DBclient.query('SELECT * FROM sample_quality_alltarget_inshot_11_'+str(r))
        full_length_sample_quality_info_df = pd.concat([full_length_sample_quality_info_df, pd.DataFrame(list(result.get_points(measurement='sample_quality_alltarget_inshot_11_'+str(r))))])
        result = DBclient.query('SELECT * FROM analy_complete_result_inshot_11_'+str(r))
        full_info_df = pd.concat([full_info_df, pd.DataFrame(list(result.get_points(measurement='analy_complete_result_inshot_11_'+str(r))))])
    
    for algo in alog_list:
        result = resultDBclient.query("SELECT * FROM video_in_server_"+algo)
        video_in_server = pd.DataFrame(list(result.get_points(measurement = "video_in_server_"+algo)))
        result = resultDBclient.query("SELECT * FROM video_nopca_in_server_"+algo)
        video_no_pca_in_server = pd.DataFrame(list(result.get_points(measurement = "video_nopca_in_server_"+algo)))

        query_result_ia = []
        query_result_ia_no_pca = []

        for q in query_video_list:
            print("Querying",q['name'],"...")
            origin_video_info = full_info_df.loc[(full_info_df['name']==q['name']) & (full_info_df['a_type']=='illegal_parking1')]['target'].iloc[0]/ MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]
            origin_video_info += full_info_df.loc[(full_info_df['name']==q['name']) & (full_info_df['a_type']=='car_counting')]['target'].iloc[0] / MaxTargetTable.loc[(MaxTargetTable['a_type']=='people_counting')]['value'].iloc[0]
            
            target_point = video_in_server.loc[video_in_server['name']==q['name']]
            ### query database with pca
            if not target_point.empty:
                target_fps = str(target_point['fps'].iloc[0]); target_bitrate = str(target_point['bitrate'].iloc[0])
            
                ### Information amount of complete videos in server
                if target_fps =='24' and target_bitrate =='1000':
                    preserved_video_info = origin_video_info
                else: ### Information amount of sampled videos in server
                    try:
                        preserved_video_info_ill1 = full_length_sample_quality_info_df.loc[(full_length_sample_quality_info_df['name']==q['name']) & (full_length_sample_quality_info_df['a_type']=='illegal_parking1') & (full_length_sample_quality_info_df['fps']==target_fps) & (full_length_sample_quality_info_df['bitrate']==target_bitrate)]['target'].iloc[0]
                        preserved_video_info_ill1 /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]
                    except:
                        print(q['name'], "fps:", target_fps, "bitrate:", target_bitrate,'ill1')
                        preserved_video_info_ill1 = 0
                    try:
                        preserved_video_info_car = full_length_sample_quality_info_df.loc[(full_length_sample_quality_info_df['name']==q['name']) & (full_length_sample_quality_info_df['a_type']=='car_counting') & (full_length_sample_quality_info_df['fps']==target_fps) & (full_length_sample_quality_info_df['bitrate']==target_bitrate)]['target'].iloc[0]
                        preserved_video_info_car /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='people_counting')]['value'].iloc[0]
                    except:
                        print(q['name'], "fps:", target_fps, "bitrate:", target_bitrate,'car')
                        preserved_video_info_car = 0
                    
                    preserved_video_info = preserved_video_info_ill1 + preserved_video_info_car
                

                info_pca_error = abs(origin_video_info-preserved_video_info)
                print("pca Find downsampled video...")
            else:
                print("pca Queried video has been deleted...")
                info_pca_error = origin_video_info
            query_result_ia.append(info_pca_error)


            ### query database with pca
            target_point = video_no_pca_in_server.loc[video_no_pca_in_server['name']==q['name']]
            if not target_point.empty:
                target_fps = str(target_point['fps'].iloc[0]); target_bitrate = str(target_point['bitrate'].iloc[0])
            
                ### Information amount of complete videos in server
                if target_fps =='24' and target_bitrate =='1000':
                    preserved_video_info = origin_video_info
                else: ### Information amount of sampled videos in server
                    try:
                        preserved_video_info_ill1 = full_length_sample_quality_info_df.loc[(full_length_sample_quality_info_df['name']==q['name']) & (full_length_sample_quality_info_df['a_type']=='illegal_parking1') & (full_length_sample_quality_info_df['fps']==target_fps) & (full_length_sample_quality_info_df['bitrate']==target_bitrate)]['target'].iloc[0]
                        preserved_video_info_ill1 /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]
                    except:
                        print(q['name'], "fps:", target_fps, "bitrate:", target_bitrate,'ill1')
                        preserved_video_info_ill1 = 0
                    try:
                        preserved_video_info_car = full_length_sample_quality_info_df.loc[(full_length_sample_quality_info_df['name']==q['name']) & (full_length_sample_quality_info_df['a_type']=='car_counting') & (full_length_sample_quality_info_df['fps']==target_fps) & (full_length_sample_quality_info_df['bitrate']==target_bitrate)]['target'].iloc[0]
                        preserved_video_info_car /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='people_counting')]['value'].iloc[0]
                    except:
                        print(q['name'], "fps:", target_fps, "bitrate:", target_bitrate,'car')
                        preserved_video_info_car = 0
                    
                    preserved_video_info = preserved_video_info_ill1 + preserved_video_info_car
                
                info_no_pca_error = abs(origin_video_info-preserved_video_info)
                print("no pca Find downsampled video...")
            else:
                print("no pca Queried video has been deleted...")
                info_no_pca_error = origin_video_info

            query_result_ia_no_pca.append(info_no_pca_error)
            print("info_error", info_pca_error, "info_error_no_pca",info_no_pca_error)

        with open('./query_ia_'+algo+'_unseen_with_pca_'+str(start_day)+'.csv','w',newline='') as f:
            writer = csv.writer(f)
            for qe in query_result_ia:
                writer.writerow([qe])

        with open('./query_ia_'+algo+'_unseen_no_pca_'+str(start_day)+'.csv','w',newline='') as f:
            writer = csv.writer(f)
            for qe in query_result_ia_no_pca:
                writer.writerow([qe])






                
                




            
                
        

                


            
