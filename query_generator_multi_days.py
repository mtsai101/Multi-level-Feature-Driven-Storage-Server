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
np.random.seed(10)

DBclient = InfluxDBClient(host=data['global']['database_ip'], port=data['global']['database_port'], database=data['global']['database_name'], username='root', password='root')
resultDBclient = InfluxDBClient(host=data['global']['database_ip'], port=data['global']['database_port'], database=data['global']['exp_database_name'], username='root', password='root')

result = DBclient.query('SELECT * FROM MaxAnalyticTargetNumber')
MaxTargetTable = pd.DataFrame(list(result.get_points(measurement="MaxAnalyticTargetNumber")))

result = DBclient.query('SELECT * FROM visual_features_entropies_PCA_normalized')
PCATable = pd.DataFrame(list(result.get_points(measurement="visual_features_entropies_PCA_normalized")))


alog_list = ['EF','EFR','FIFO','approx','heuristic','opt']


SEEN_ANALY_LIST = ["illegal_parking0", "people_counting"]
UNSEEN_ANALY_LIST = ["illegal_parking1", "car_counting"]

if __name__=='__main__':

    start_day = 9
    end_day = 15
    size  = (end_day-start_day+1)
    rng = default_rng()
    full_length_sample_quality_info_df = None
    full_info_df = None

    for r in range(size):
        query_video_list = []
        chosen_ana_list = []
        print("Generate queries...Day ",r)
        if os.path.isfile(f'./experiments/query_ia_dayserror_allalgo_day{r}.csv'):
            os.remove(f'./experiments/query_ia_dayserror_allalgo_day{r}.csv')

        date = str(r + start_day)
        result = DBclient.query("SELECT * FROM raw_11_"+str(date))
        per_day_video_list = list(result.get_points(measurement="raw_11_"+str(date)))
        video_num_per_day = len(per_day_video_list)
        poisson_query = np.random.poisson(lam=8/video_num_per_day, size=video_num_per_day) # 8 request / 24 hour 


        for idx_q, num_q in enumerate(poisson_query):
            if num_q == 0:
                continue
            chosen_ana_list.append(rng.choice(len(SEEN_ANALY_LIST), num_q ,replace=True))
            query_video_list.append(per_day_video_list[idx_q])

        result = DBclient.query('SELECT * FROM sample_quality_alltarget_inshot_11_'+str(date))
        full_length_sample_quality_info_df = pd.concat([full_length_sample_quality_info_df, pd.DataFrame(list(result.get_points(measurement='sample_quality_alltarget_inshot_11_'+str(date))))])
        result = DBclient.query('SELECT * FROM analy_complete_result_inshot_11_'+str(date))
        full_info_df = pd.concat([full_info_df, pd.DataFrame(list(result.get_points(measurement='analy_complete_result_inshot_11_'+str(date))))])
    
        for algo in alog_list:
            result = resultDBclient.query("SELECT * FROM video_in_server_"+algo)
            video_in_server = pd.DataFrame(list(result.get_points(measurement = "video_in_server_"+algo)))
            query_result_ia = []
            for q in query_video_list:
                # information amount of original video
                print("Querying",q['name'],"...")
                origin_video_info = (full_info_df.loc[(full_info_df['name']==q['name']) & (full_info_df['a_type']=='illegal_parking0')]['target'].iloc[0] / MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]) 
                origin_video_info += (full_info_df.loc[(full_info_df['name']==q['name']) & (full_info_df['a_type']=='people_counting')]['target'].iloc[0] / MaxTargetTable.loc[(MaxTargetTable['a_type']=='people_counting')]['value'].iloc[0]) 
                origin_video_info += PCATable.loc[PCATable['name']==q['name']].iloc[0]['value']
            
                target_point = video_in_server.loc[video_in_server['name']==q['name']]
                
                if not target_point.empty:
                    target_fps = str(target_point['fps'].iloc[0]); target_bitrate = str(target_point['bitrate'].iloc[0])
                
                    ### Information amount of complete videos in server
                    if target_fps =='24' and target_bitrate =='1000':
                        preserved_video_info = origin_video_info
                    else: ### Information amount of sampled videos in server
                        try:
                            preserved_video_info_ill0 = full_length_sample_quality_info_df.loc[(full_length_sample_quality_info_df['name']==q['name']) & (full_length_sample_quality_info_df['a_type']=='illegal_parking0') & (full_length_sample_quality_info_df['fps']==target_fps) & (full_length_sample_quality_info_df['bitrate']==target_bitrate)]['target'].iloc[0]
                            preserved_video_info_ill0 /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='illegal_parking0')]['value'].iloc[0]
                        except:
                            print(q['name'], "fps:", target_fps, "bitrate:", target_bitrate,'ill')
                            preserved_video_info_ill0 = 0
                        try:
                            preserved_video_info_peo = full_length_sample_quality_info_df.loc[(full_length_sample_quality_info_df['name']==q['name']) & (full_length_sample_quality_info_df['a_type']=='people_counting') & (full_length_sample_quality_info_df['fps']==target_fps) & (full_length_sample_quality_info_df['bitrate']==target_bitrate)]['target'].iloc[0]
                            preserved_video_info_peo /= MaxTargetTable.loc[(MaxTargetTable['a_type']=='people_counting')]['value'].iloc[0]
                        except:
                            print(q['name'], "fps:", target_fps, "bitrate:", target_bitrate,'peo')
                            preserved_video_info_peo = 0
                        try:
                            preserved_video_info_pca = PCATable.loc[PCATable['name']==q['name']].iloc[0]['value']
                        except:
                            preserved_video_info_pca = 0
                        preserved_video_info = preserved_video_info_ill0 + preserved_video_info_peo + preserved_video_info_pca
                    
                    info_error = abs(origin_video_info-preserved_video_info)
                else:
                    print("Queried video has been deleted...")
                    info_error = origin_video_info

                query_result_ia.append(info_error)

            with open(f'./experiments/query_ia_dayserror_allalgo_day{r}.csv','a',newline='') as f:
                writer = csv.writer(f)
                writer.writerow([sum(query_result_ia)/len(query_result_ia), max(query_result_ia)])






                
                




            
                
        

                


            
