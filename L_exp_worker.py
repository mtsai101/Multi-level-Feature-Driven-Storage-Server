import csv
import numpy as np
import yaml
import sys
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)
from optimal_downsampling_manager.resource_predictor.estimate_table import get_context, get_month_and_day
from influxdb import InfluxDBClient
DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', "exp_storage")


### Hours <--> Information Amount
# result = DBclient.query("SELECT * FROM L_heuristic_exp_time_profit")
# result_list = list(result.get_points(measurement = "L_heuristic_exp_time_profit"))
# with open("experiments/L_heuristic_info_hours.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for r in result_list:
#         writer.writerow([r['profit_sum']])
# ### Hours <--> Analytime sum
# with open("experiments/L_heuristic_anatime_hours.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for r in result_list:
#         writer.writerow([r['time_sum']])


# ### Hours <--> Information Amount
# result = DBclient.query("SELECT * FROM L_opt_exp_time_profit")
# result_list = list(result.get_points(measurement = "L_opt_exp_time_profit"))
# with open("experiments/L_opt_info_hours.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for r in result_list:
#         writer.writerow([r['profit_sum']])

# ### Hours <--> Analytime sum
# result = DBclient.query("SELECT * FROM L_opt_exp_time_profit")
# result_list = list(result.get_points(measurement = "L_opt_exp_time_profit"))
# with open("experiments/L_opt_anatime_hours.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for r in result_list:
#         writer.writerow([r['time_sum']])

# ## Sampling length scatter
# result = DBclient.query("SELECT * FROM L_opt_exp_length")
# result_list = list(result.get_points(measurement = "L_opt_exp_length"))
# with open("experiments/L_opt_length_scatter_11_11_0_5.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for r in result_list:
#         day_idx, time_idx = get_context(r['name'])
#         month, date = get_month_and_day(r['name'])
#         if date == 11 and 0<=time_idx and time_idx<=5:
#             writer.writerow([time_idx, r['ill_param'], r['peo_param']])
#         if date == 11 and 19<=time_idx and time_idx<=23:
#             writer.writerow([time_idx, r['ill_param'], r['peo_param']])


### Hours <--> Algorithm running time
# result = DBclient.query("SELECT * FROM L_heuristic_exp_time_profit")
# result_list = list(result.get_points(measurement = "L_heuristic_exp_time_profit"))

# weekend = ['7','8','14','15']
# ti=[[] for i in range(4)]
# for r in result_list:

#     if (r['day'] not in weekend):
#         if r['start']=='0':
#             ti[0].append(r['execution_time'])
#         elif r['start']=='6':
#             ti[1].append(r['execution_time'])
#         elif r['start']=='12':
#             ti[2].append(r['execution_time'])
#         elif r['start']=='18':
#             ti[3].append(r['execution_time'])

# ti = np.array(ti)
# print(ti)
# err = np.zeros(4)
# avg = np.mean(ti, axis=1)

# for i in range(4):
#     err[i] = 1.96*(np.std(ti[i])/ti.shape[1])
# print(avg)
# print(err)

# with open("experiments/L_heuristic_algo_running_time_weekday.csv",'w') as csvfile:
#     writer = csv.writer(csvfile)
#     for i in range(4):
#         writer.writerow([avg[i],err[i]])