import csv
import numpy as np
import yaml
import sys
with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)
from optimal_downsampling_manager.resource_predictor.estimate_table import get_context, get_month_and_day
from influxdb import InfluxDBClient
pre_a_selected=[1,24,48,96,144,0]
pre_a_selected_tuple = []
for a in pre_a_selected:
    for b in pre_a_selected:
        pre_a_selected_tuple.append((a,b)) 
DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database_port'], 'root', 'root', "exp_storage")
weekend = ['7','8','14','15']
# algo_list = ["opt","heuristic","approx"]
algo_list = ['gt']

for algo in algo_list:
    # ### Hours <--> Information Amount 
    # result = DBclient.query("SELECT * FROM L_"+algo+"_exp_time_profit")
    # result_list = list(result.get_points(measurement = "L_"+algo+"_exp_time_profit"))

    # weekday_ti=[[] for i in range(4)]
    # weekend_ti=[[] for i in range(4)]

    # for r in result_list:
    #     if (r['day'] not in weekend): ## weekday
    #         if r['start']=='0':
    #             weekday_ti[0].append(r['profit_sum'])
    #         elif r['start']=='6':
    #             weekday_ti[1].append(r['profit_sum'])
    #         elif r['start']=='12':
    #             weekday_ti[2].append(r['profit_sum'])
    #         elif r['start']=='18':
    #             weekday_ti[3].append(r['profit_sum'])
    #     elif (r['day'] in weekend): ## weekend
    #         if r['start']=='0':
    #             weekend_ti[0].append(r['profit_sum'])
    #         elif r['start']=='6':
    #             weekend_ti[1].append(r['profit_sum'])
    #         elif r['start']=='12':
    #             weekend_ti[2].append(r['profit_sum'])
    #         elif r['start']=='18':
    #             weekend_ti[3].append(r['profit_sum'])

    # weekday_ti = np.array(weekday_ti); weekend_ti = np.array(weekend_ti)
    # print("weekday_ti",weekday_ti); print("weekend_ti",weekend_ti)
    # weekday_err = np.zeros(4); weekday_avg = np.mean(weekday_ti, axis=1)
    # weekend_err = np.zeros(4); weekend_avg = np.mean(weekend_ti, axis=1)

    # for i in range(4):
    #     weekday_err[i] = 1.96*(np.std(weekday_ti[i])/pow(weekday_ti.shape[1],0.5))
    #     weekend_err[i] = 1.96*(np.std(weekend_ti[i])/pow(weekend_ti.shape[1],0.5))

    # with open("experiments/L_"+algo+"_info_hours_weekday.csv",'w') as csvfile:
    #     writer = csv.writer(csvfile)
    #     for i in range(4):
    #         writer.writerow([weekday_avg[i],weekday_err[i]])
    # with open("experiments/L_"+algo+"_info_hours_weekend.csv",'w') as csvfile:
    #     writer = csv.writer(csvfile)
    #     for i in range(4):
    #         writer.writerow([weekend_avg[i],weekend_err[i]])



    ### Hours <--> Analytime sum
    result = DBclient.query("SELECT * FROM L_"+algo+"_exp_time_profit")
    result_list = list(result.get_points(measurement = "L_"+algo+"_exp_time_profit"))

    weekday_ti=[[] for i in range(4)]
    weekend_ti=[[] for i in range(4)]

    for r in result_list:
        if (r['day'] not in weekend): ## weekday
            if r['start']=='0':
                weekday_ti[0].append(r['time_sum'])
            elif r['start']=='6':
                weekday_ti[1].append(r['time_sum'])
            elif r['start']=='12':
                weekday_ti[2].append(r['time_sum'])
            elif r['start']=='18':
                weekday_ti[3].append(r['time_sum'])
        elif (r['day'] in weekend): ## weekend
            if r['start']=='0':
                weekend_ti[0].append(r['time_sum'])
            elif r['start']=='6':
                weekend_ti[1].append(r['time_sum'])
            elif r['start']=='12':
                weekend_ti[2].append(r['time_sum'])
            elif r['start']=='18':
                weekend_ti[3].append(r['time_sum'])

    weekday_ti = np.array(weekday_ti); weekend_ti = np.array(weekend_ti)
    print("weekday_ti",weekday_ti); print("weekend_ti",weekend_ti)
    weekday_err = np.zeros(4); weekday_avg = np.mean(weekday_ti, axis=1)
    weekend_err = np.zeros(4); weekend_avg = np.mean(weekend_ti, axis=1)

    for i in range(4):
        weekday_err[i] = 1.96*(np.std(weekday_ti[i])/pow(weekday_ti.shape[1],0.5))
        weekend_err[i] = 1.96*(np.std(weekend_ti[i])/pow(weekend_ti.shape[1],0.5))

    with open("experiments/L_"+algo+"_anatime_hours_weekday.csv",'w') as csvfile:
        writer = csv.writer(csvfile)
        for i in range(4):
            writer.writerow([weekday_avg[i],weekday_err[i]])
    with open("experiments/L_"+algo+"_anatime_hours_weekend.csv",'w') as csvfile:
        writer = csv.writer(csvfile)
        for i in range(4):
            writer.writerow([weekend_avg[i],weekend_err[i]])


    ## Sampling length scatter
    # result = DBclient.query("SELECT * FROM L_"+algo+"_exp_length")
    # result_list = list(result.get_points(measurement = "L_"+algo+"_exp_length"))
    # length_dict = [dict() for i in range(4)]
    # for i in range(4):
    #     for j in pre_a_selected_tuple:
    #         length_dict[i][j] = 0

    # for r in result_list:
    #     day_idx, time_idx = get_context(r['name'])
    #     month, date = get_month_and_day(r['name'])
    #     if 0<=time_idx and time_idx<6:
    #         length_dict[0][(r['ill_param'], r['peo_param'])]+=1
    #     elif 6<=time_idx and time_idx<12:
    #         length_dict[1][(r['ill_param'], r['peo_param'])]+=1
    #     elif 12<=time_idx and time_idx<18:
    #         length_dict[2][(r['ill_param'], r['peo_param'])]+=1
    #     elif 18<=time_idx and time_idx<24:
    #         length_dict[3][(r['ill_param'], r['peo_param'])]+=1

    #     with open("experiments/L_"+algo+"_length_scatter.csv",'w') as csvfile:
    #         for i in pre_a_selected_tuple:
    #             writer = csv.writer(csvfile)
    #             writer.writerow([length_dict[0][i],length_dict[1][i],length_dict[2][i],length_dict[3][i]])
    

            
    # ### Hours <--> Algorithm running time
    # result = DBclient.query("SELECT * FROM L_"+algo+"_exp_time_profit")
    # result_list = list(result.get_points(measurement = "L_"+algo+"_exp_time_profit"))

    # weekday_ti=[[] for i in range(4)]
    # weekend_ti=[[] for i in range(4)]

    # for r in result_list:
    #     if (r['day'] not in weekend): ## weekday
    #         if r['start']=='0':
    #             weekday_ti[0].append(r['execution_time'])
    #         elif r['start']=='6':
    #             weekday_ti[1].append(r['execution_time'])
    #         elif r['start']=='12':
    #             weekday_ti[2].append(r['execution_time'])
    #         elif r['start']=='18':
    #             weekday_ti[3].append(r['execution_time'])
    #     elif (r['day'] in weekend): ## weekend
    #         if r['start']=='0':
    #             weekend_ti[0].append(r['execution_time'])
    #         elif r['start']=='6':
    #             weekend_ti[1].append(r['execution_time'])
    #         elif r['start']=='12':
    #             weekend_ti[2].append(r['execution_time'])
    #         elif r['start']=='18':
    #             weekend_ti[3].append(r['execution_time'])

    # weekday_ti = np.array(weekday_ti); weekend_ti = np.array(weekend_ti)
    # # weekday_ti = np.log10(weekday_ti); weekend_ti = np.log10(weekend_ti)
    # weekday_err = np.zeros(4); weekday_avg = np.mean(weekday_ti, axis=1)
    # weekend_err = np.zeros(4); weekend_avg = np.mean(weekend_ti, axis=1)
    # print("weekday_ti",weekday_ti); print("weekend_ti",weekend_ti)

    # for i in range(4):
    #     weekday_err[i] = 1.96*(np.std(weekday_ti[i])/pow(weekday_ti.shape[1], 0.5))
    #     weekend_err[i] = 1.96*(np.std(weekend_ti[i])/pow(weekend_ti.shape[1], 0.5))

    # with open("experiments/L_"+algo+"_algo_running_time_weekday.csv",'w') as csvfile:
    #     writer = csv.writer(csvfile)
    #     for i in range(4):
    #         writer.writerow([weekday_avg[i],weekday_err[i]])
    # with open("experiments/L_"+algo+"_algo_running_time_weekend.csv",'w') as csvfile:
    #     writer = csv.writer(csvfile)
    #     for i in range(4):
    #         writer.writerow([weekend_avg[i],weekend_err[i]])