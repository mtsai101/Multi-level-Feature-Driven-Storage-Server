import csv
import numpy as np
import yaml
import sys
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)
from optimal_downsampling_manager.resource_predictor.estimate_table import get_context, get_month_and_day
from influxdb import InfluxDBClient
DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', "exp_storage")

## Hour <--> IA
# algo = 'heuristic'
# result = DBclient.query("SELECT * FROM log_every_hour_"+algo)
# result_list = list(result.get_points(measurement = "log_every_hour_"+algo))
# count=0
# prev_amount = 0
# with open("experiments/P_"+algo+"_ia_hours.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for r in result_list:
#         while count != int(r['hour']):
#             writer.writerow([prev_amount])
#             count = (count+1)%24            

#         writer.writerow([r['total_ia']])
#         prev_amount = float(r['total_ia'])
#         count = (count+1)%24

## Hour <--> clips
# algo = 'heuristic'
# result = DBclient.query("SELECT * FROM log_every_hour_"+algo)
# result_list = list(result.get_points(measurement = "log_every_hour_"+algo))
# count=0
# prev_amount = 0
# with open("experiments/P_"+algo+"_clips_hours.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for r in result_list:
#         while count != int(r['hour']):
#             writer.writerow([prev_amount])
#             count = (count+1)%24            

#         writer.writerow([r['total_clips_number']])
#         prev_amount = int(r['total_clips_number'])
#         count = (count+1)%24

### Hour <--> space
# algo = 'heuristic'
# result = DBclient.query("SELECT * FROM log_every_hour_"+algo)
# result_list = list(result.get_points(measurement = "log_every_hour_"+algo))
# count=0
# prev_amount = 0
# with open("experiments/P_"+algo+"_space_hours.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for r in result_list:
#         while count != int(r['hour']):
#             writer.writerow([prev_amount])
#             count = (count+1)%24            

#         writer.writerow([r['total_size']])
#         prev_amount = int(r['total_size'])
#         count = (count+1)%24

### Hour <--> Downsampling Time
### Direct write