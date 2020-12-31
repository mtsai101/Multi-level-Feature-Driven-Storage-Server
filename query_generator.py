from influxdb import InfluxDBClient
import numpy as np
import random
import csv 
import os
from optimal_downsampling_manager.resource_predictor.estimate_table import Degraded_IATable, get_context, DownTimeTable, DownRatioTable, Degraded_Q_IATable, get_month_and_day

import yaml
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', data['global']['database_name'])
resultDBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', 'exp_storage')



if __name__=='__main__':


    # universal_dataset = []
    # for r in range(start_day,end_day+1):
    #     result = DBclient.query("SELECT * FROM raw_11_"+str(r)+"_analy_all")
    #     universal_dataset_per_day = list(result.get_points(measurement="raw_11_"+str(r)+"_analy_all"))
    #     hour_list = [[] for i in range(24)]
    #     for c in universal_dataset_per_day:
    #         hour = int(c['name'].split("/")[-1].split("_")[-1].split(":")[0])
    #         hour_list[hour].append(c['name'])
    #     universal_dataset.append(hour_list)
result = DBclient.query("SELECT * FROM log_every_hour_"+algo)
result_list = list(result.get_points(measurement = "log_every_hour_"+algo))
    start_day=9, 16
    for sim_day in range(start_day, end_day):
        pending_list = []
        size  = (sim_day-start_day+1)
        poisson_peo_rate = np.random.poisson(10,size) # 10 request / 24 hour 
        poisson_ill_rate = np.random.poisson(10,size) # 10 request / 24 hour 

        # with open('./query_pending_list/'+str(sim_day)+'.csv','w',newline='') as f:
        #     writer = csv.writer(f)

        #     for k_p, p in enumerate(peo_rate):
        #         tmp_list = universal_dataset[int(k_p/24)][k_p%24]
        #         pending_list.extend(random.sample(tmp_list,k=min(len(tmp_list),p)))
        #     for pend in pending_list:
        #         writer.writerow([pend])






            
            




        
            
    

            


        
