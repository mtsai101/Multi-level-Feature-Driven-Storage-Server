from influxdb import InfluxDBClient
import datetime
import pandas as pd
import numpy as np
import time
import yaml
import sys

ANALY_LIST=['illegal_parking0','people_counting']
DOWN_LIST=['temporal','bitrate','qp']

pre_a_selected=[24,48,96,144]

pre_d_selected = [(24,1000),(24,500),(12,500),(12,100),(6,100),(6,10),(1,10)]



with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)

DBclient = InfluxDBClient(data['global']['database_ip'], data['global']['database'], 'root', 'root', 'storage')


class Table:
    def __init__(self,table_name):
        self.table_name = table_name
        self.warm_up = 100
        self.last_update_time = 0
        self.window_size = 6
        self.result_point = list()
        self.analy_result_table = None
        self.down_result_table = None
        self.start_day = 4
        self.end_day = 9
        

    # def get_latest_value(self, day_idx, time_idx, a_type='None', a_parameter=-1, fps=24, bitrate=1000):
    #     # day_idx, time_idx = get_context(clip_name)
    #     """
    #     step1 : check the very time is in weekday or weekend
    #     step2 : check nearest, switch to another weektime every time slot.
    #     """
    #     for r in range(0,11): # 
    #         for w in range(2):# check the corresponding time in weekend
    #             for i in range(2*r,2*r+2): # check neighbor which time difference is +1 or -1 
    #                 step = int(i/2) * pow((-1),i)
    #                 new_time_idx = (time_idx + step + self.window_size) % self.window_size
                    
    #                 target_col, estimation_point = self.make_query(day_idx,new_time_idx, a_type, a_parameter, fps, bitrate)
    #                 if estimation_point.shape[0]==self.window_size:
    #                     estimation = estimation_point.loc[:,target_col].mean()
    #                     return estimation
                    
    #             day_idx = (day_idx+1) % 2
    #     return 0.0

    def get_latest_value(self, day_idx, time_idx, a_type='None', a_parameter=-1, fps=24, bitrate=1000):
        return_value = self.make_query(day_idx, time_idx, a_type, a_parameter, fps, bitrate)
        return return_value

    def make_query(self, day_idx, time_idx, a_type, a_parameter, fps, bitrate):

        if self.model_type == 'Full_IATable': #\hat{e}(c,a,f_c)
            result_list = []; 
            for d in range(self.start_day,self.end_day):
                table_name = 'analy_complete_result_inshot_11_'+str(d)
                # print('SELECT \"name\", target/total_frame_number FROM '+ table_name +' WHERE \"a_type\"=\''+ a_type +'\' AND \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx)+'\'')
                try:
                    result = DBclient.query('SELECT \"name\", target/total_frame_number FROM '+ table_name +' WHERE \"a_type\"=\''+ a_type +'\' AND \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx)+'\'')
                    result_list.extend(list(result.get_points(measurement = table_name)))

                except:
                    print("Miss some 11_"+str(d)+" time_idx:", time_idx, "a_type:", a_type," but whatever...")
                    continue

            if len(result_list)>0:
                sorted_list = sorted(result_list, key=lambda k :k['name'], reverse=True)
                # print(sum(sorted_list[:]['target_total_frame_number']))
                if len(sorted_list) > self.window_size:
                    result_value = sum(item['target_total_frame_number'] for item in sorted_list[:self.window_size+1])/self.window_size
                else:
                    result_value = sum(item['target_total_frame_number'] for item in sorted_list[:len(sorted_list)+1])/len(sorted_list)
                return result_value / self.max_info[a_type]
            else:
                return 0

        elif self.model_type == 'degraded_IATable':
            sample_result_list=[]; full_result_list=[]
            for d in range(self.start_day,self.end_day):
                full_table_name = 'analy_complete_result_inshot_11_'+str(d)
                sample_table_name = 'analy_sample_result_inshot_11_'+str(d)
                try:
                    sample_result = DBclient.query('SELECT \"name\", target FROM '+ sample_table_name +' WHERE \"a_type\"=\''+ a_type +'\' AND \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx)+'\' AND \"a_parameter\"=\''+str(a_parameter)+'\'')
                    sample_result_list.extend(list(sample_result.get_points(measurement = sample_table_name)))
                    full_result = DBclient.query('SELECT \"name\", target FROM '+ full_table_name +' WHERE \"a_type\"=\''+ a_type +'\' AND \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx)+'\'')
                    full_result_list.extend(list(full_result.get_points(measurement = full_table_name)))
                    if len(sample_result) != len(full_result):
                        print("not found enough sample_result_list on", sample_table_name, "")
                except:
                    continue

            if len(sample_result_list)>0 and len(full_result_list)>0:
                result_value=0
                full_sorted_list = sorted(full_result_list, key=lambda k :k['name'], reverse=True)
                sample_sorted_list = sorted(sample_result_list, key=lambda k :k['name'], reverse=True)
            
                length = min(len(full_sorted_list), self.window_size)
                for i in range(length):
                    if full_sorted_list[i]['target'] and sample_sorted_list[i]['target']:
                        result_value += sample_sorted_list[i]['target']/full_sorted_list[i]['target']
                return result_value/length
            else:
                return 0

        elif self.model_type == 'AnalyTime':
            result_list = []; 
            for d in range(self.start_day, self.end_day):
                table_name = "analy_complete_result_inshot_11_" +str(d)  # For t(c,a)
                result = DBclient.query('SELECT \"name\", total_frame_number, time_consumption/total_frame_number FROM '+table_name+' WHERE \"a_type\"=\'' + a_type + '\' AND \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx)+'\'')
                result_list.extend(list(result.get_points(measurement = table_name)))

            if len(result_list)>0:
                sorted_list = sorted(result_list, key=lambda k :k['name'], reverse=True)
                # print(sum(sorted_list[:]['target_total_frame_number']))
                if len(sorted_list) > self.window_size:
                    result_value = sum(item['time_consumption_total_frame_number'] for item in sorted_list[:self.window_size+1])/self.window_size
                    # print("total frame:",sum(item['total_frame_number'] for item in sorted_list[:self.window_size+1])/self.window_size)

                else:
                    result_value = sum(item['time_consumption_total_frame_number'] for item in sorted_list[:len(sorted_list)+1])/len(sorted_list)
                    # print(sum(item['total_frame_number'] for item in sorted_list[:len(sorted_list)+1])/len(sorted_list))

                return result_value
            else:
                return 0

        elif self.model_type == 'DownTime':
            # For t(c, P_c)
            result_list = []; 
            table_name = "down_result" 
            # print('SELECT \"name\", execution_time FROM ' + table_name+ ' WHERE ' + '\"day_idx\"=\'' + str(day_idx) + '\' AND \"time_idx\"=\'' + str(time_idx)+'\' AND \'fps\'=\''+str(fps)+'\' AND \'bitrate\'='+str(bitrate)+'\'')
            result = DBclient.query('SELECT \"name\", execution_time FROM ' + table_name+ ' WHERE ' + '\"day_idx\"=\'' + str(day_idx) + '\' AND \"time_idx\"=\'' + str(time_idx)+'\' AND \"fps\"=\''+str(fps)+'\' AND \"bitrate\"=\''+str(bitrate)+'\'')
            result_list.extend(list(result.get_points(measurement = table_name)))

            if len(result_list)>0:
                sorted_list = sorted(result_list, key=lambda k :k['name'], reverse=True)
                # print(sum(sorted_list[:]['target_total_frame_number']))
                if len(sorted_list) > self.window_size:
                    result_value = sum(item['execution_time'] for item in sorted_list[:self.window_size+1])/self.window_size
                    # print("total frame:",sum(item['total_frame_number'] for item in sorted_list[:self.window_size+1])/self.window_size)

                else:
                    result_value = sum(item['execution_time'] for item in sorted_list[:len(sorted_list)+1])/len(sorted_list)
                    # print(sum(item['total_frame_number'] for item in sorted_list[:len(sorted_list)+1])/len(sorted_list))

                return result_value
            else:
                return 0

        elif self.model_type == 'DownRatio':
            # For \hat{o}(c, P_c)
            result_list = []; 
            table_name = "down_result" 
            # print('SELECT \"name\", execution_time FROM ' + table_name+ ' WHERE ' + '\"day_idx\"=\'' + str(day_idx) + '\' AND \"time_idx\"=\'' + str(time_idx)+'\' AND \'fps\'=\''+str(fps)+'\' AND \'bitrate\'='+str(bitrate)+'\'')
            result = DBclient.query('SELECT \"name\", ratio FROM ' + table_name+ ' WHERE ' + '\"day_idx\"=\'' + str(day_idx) + '\' AND \"time_idx\"=\'' + str(time_idx)+'\' AND \"fps\"=\''+str(fps)+'\' AND \"bitrate\"=\''+str(bitrate)+'\'')
            result_list.extend(list(result.get_points(measurement = table_name)))

            if len(result_list)>0:
                sorted_list = sorted(result_list, key=lambda k :k['name'], reverse=True)
                # print(sum(sorted_list[:]['target_total_frame_number']))
                if len(sorted_list) > self.window_size:
                    result_value = sum(item['ratio'] for item in sorted_list[:self.window_size+1])/self.window_size
                    # print("total frame:",sum(item['total_frame_number'] for item in sorted_list[:self.window_size+1])/self.window_size)

                else:
                    result_value = sum(item['ratio'] for item in sorted_list[:len(sorted_list)+1])/len(sorted_list)
                    # print(sum(item['total_frame_number'] for item in sorted_list[:len(sorted_list)+1])/len(sorted_list))

                return result_value
            else:
                return 0
        else:
            raise NameError('Unknown Query Name')
        return 0
        
class AnalyTimeTable(Table):
    def __init__(self,refresh):
        super().__init__('')
        self.model_type='AnalyTime'
        self.refresh = refresh
        if self.refresh:
            print("[INFO] Updating the AnalyTimeTable models...")
            drop_measurement_if_exist("AnalyTimeTable")
            for day_idx in range(2):
                for time_idx in range(24):
                    for a_type in ANALY_LIST:
                        # print("day_idx:", day_idx," time_idx",time_idx," a_type:", a_type)
                        value = self.get_latest_value(day_idx, time_idx, a_type=a_type)
                        # print("value", value)
                        json_body = [
                                {
                                    "measurement":self.model_type + 'Table',
                                    "tags": {
                                        "name": self.model_type,
                                        "a_type":a_type,
                                        "day_of_week":day_idx,
                                        "time_of_day":time_idx
                                    },
                                    "fields": {
                                        "value":value
                                    }
                                }
                            ]

                        DBclient.write_points(json_body)
            print("[INFO] Updating completed!")


        

class DownTimeTable(Table):
    def __init__(self,refresh):
        super().__init__('down_result')
        self.model_type='DownTime'
        self.refresh = refresh
        if self.refresh:
            print("[INFO] Updating the DownTimeTable models...")
            drop_measurement_if_exist("DownTimeTable")
            json_body=[]
            for day_idx in range(2):
                for time_idx in range(24):
                    for d_param_key, d_parameter in enumerate(pre_d_selected):
                        if d_param_key==0:
                            value = 0
                        else:
                            value = self.get_latest_value(day_idx, time_idx, fps=d_parameter[0], bitrate=d_parameter[1])
                        json_body.append(
                                {
                                    "measurement":self.model_type + 'Table',
                                    "tags": {
                                        "name": self.model_type,
                                        "fps":float(d_parameter[0]),
                                        "bitrate":float(d_parameter[1]),
                                        "day_of_week":int(day_idx),
                                        "time_of_day":int(time_idx),
                                    },
                                    "fields": {
                                        "value":float(value)
                                    }
                                }
                        )      
            DBclient.write_points(json_body, database='storage', time_precision='ms', batch_size=40000, protocol='json')

            # DBclient.write_points(json_body)
            print("[INFO] Updating completed!")

                            
class DownRatioTable(Table):
    def __init__(self,refresh):
        super().__init__('down_result')
        self.model_type='DownRatio'
        self.refresh =refresh
        if self.refresh:
            print("[INFO] Updating the DownRatioTable models...")
            drop_measurement_if_exist("DownRatioTable")
            json_body = []
            for day_idx in range(2):
                for time_idx in range(24):
                    for d_param_key, d_parameter in enumerate(pre_d_selected):
                        if d_param_key==0:
                            value = 1
                        else:
                            value = self.get_latest_value(day_idx, time_idx, fps=d_parameter[0], bitrate=d_parameter[1])
                        json_body.append(
                                {
                                    "measurement":self.model_type + 'Table',
                                    "tags": {
                                        "name": self.model_type,
                                        "fps":float(d_parameter[0]),
                                        "bitrate":float(d_parameter[1]),
                                        "day_of_week":int(day_idx),
                                        "time_of_day":int(time_idx),
                                    },
                                    "fields": {
                                        "value":float(value)
                                    }
                                }
                        )      
            print(len(json_body))
            DBclient.write_points(json_body, database='storage', time_precision='ms', batch_size=40000, protocol='json')

            print("[INFO] Updating completed!")
    
class Degraded_IATable(Table):
    def __init__(self,refresh):
        super().__init__('')

        self.model_type='degraded_IATable'
        self.refresh = refresh
        if self.refresh:
            ## influxdb can not update, only we can do is deleting the previous one
            print("[INFO] Updating the Degraded_IATable models...")
            drop_measurement_if_exist("Degraded_IATable")
            json_body = []
            for day_idx in range(2):
                for time_idx in range(24):
                    for a_type in ANALY_LIST:
                        for a_parameter in pre_a_selected:
                            value = self.get_latest_value(day_idx, time_idx, fps=d_parameter[0], bitrate=d_parameter[1])
                            json_body = [
                                    {
                                        "measurement":self.model_type,
                                        "tags": {
                                            "name": self.model_type,
                                            "a_type":a_type,
                                            "fps":float(d_parameter[0]),
                                            "bitrate":float(d_parameter[1]),
                                            "day_of_week":int(day_idx),
                                            "time_of_day":int(time_idx),
                                        },
                                        "fields": {
                                            "value":float(value)
                                        }
                                    }
                                ]
            DBclient.write_points(json_body, database='storage', time_precision='ms', batch_size=40000, protocol='json')
            print("[INFO] Updating completed!")
        


class Degraded_IATable(Table):
    def __init__(self,refresh):
        super().__init__('')

        self.model_type='degraded_IATable'
        self.refresh = refresh
        if self.refresh:
            ## influxdb can not update, only we can do is deleting the previous one
            print("[INFO] Updating the Degraded_IATable models...")
            drop_measurement_if_exist("Degraded_IATable")
            for day_idx in range(2):
                for time_idx in range(24):
                    for a_type in ANALY_LIST:
                        for a_parameter in pre_a_selected:
                            value = self.get_latest_value(day_idx, time_idx, a_type=a_type, a_parameter = a_parameter)
                            json_body = [
                                    {
                                        "measurement":self.model_type,
                                        "tags": {
                                            "name": self.model_type,
                                            "a_type":a_type,
                                            "a_param":a_parameter,
                                            "day_of_week":day_idx,
                                            "time_of_day":time_idx,
                                        },
                                        "fields": {
                                            "value":value
                                        }
                                    }
                                ]
                            DBclient.write_points(json_body)
            print("[INFO] Updating completed!")
        

        
class Full_IATable(Table):
    def __init__(self,refresh):
        super().__init__('')

        self.model_type='Full_IATable'
        self.max_info={"illegal_parking0":0, "people_counting":0}
        self.refresh = refresh
         
        # store max_value
        if self.refresh:
            for d in range(self.start_day, self.end_day):
                building_table_name = 'analy_complete_result_inshot_11_'+str(d)
                for a_type in ANALY_LIST:

                    max_result = DBclient.query('SELECT target/total_frame_number AS info FROM ' + building_table_name + ' WHERE \"a_type\"=\''+a_type+'\'')
                    max_result = list(max_result.get_points(measurement = building_table_name))
                    max_value = sorted(max_result, key=lambda k :k['info'],reverse=True)[0]['info']

                    self.max_info[a_type] = max(self.max_info[a_type], max_value)

            ## influxdb can not update, only we can do is deleting the previous one
            print("[INFO] Updating the Full_IATable models...")
            drop_measurement_if_exist("Full_IATable")
            for day_idx in range(2):
                for time_idx in range(24):
                    for a_type in ANALY_LIST:
                        value = self.get_latest_value(day_idx, time_idx, a_type=a_type)
                        json_body = [
                                {
                                    "measurement":self.model_type,
                                    "tags": {
                                        "name": self.model_type,
                                        "a_type":a_type,
                                        "day_of_week":day_idx,
                                        "time_of_day":time_idx,
                                    },
                                    "fields": {
                                        "value":value
                                    }
                                }
                            ]
                        DBclient.write_points(json_body)
                        
            print("[INFO] Updating completed!")
        
        # result = DBclient.query('SELECT * FROM IAPredTable')
        # result_point = list(result.get_points(measurement='IAPredTable'))
        # self.table = np.zeros(len(result_point), dtype = np.float32)

        # for k,i in enumerate(result_point):
        #     self.table[k] =  i["value"]

        # self.table = self.table.reshape((2,12,len(ANALY_LIST),len(pre_a_selected)+len(pre_d_selected)))
        

    def get_estimation(self,day_of_week=0,time_of_day=0,a_type=-1,a_parameter=-1, p_id=-1):
        if p_id==-1:
            return self.table[day_of_week][time_of_day][a_type][a_parameter]
        else:
            return self.table[day_of_week][time_of_day][a_type][len(pre_a_selected)+p_id]

    
            




def get_context(clip_name):
    video_name_list = clip_name.split('/')[-1].split('_')    
    day_idx = datetime.datetime.strptime(video_name_list[-2], '%Y-%m-%d')
    day_idx = int(day_idx.weekday() >= 5) # day_idx==0 if weekday else day_idx==1
    time_idx = int(int(video_name_list[-1].split('-')[0]))
    return day_idx, time_idx

def drop_measurement_if_exist(table_name):
    result = DBclient.query('SELECT * FROM '+table_name)
    result_point = list(result.get_points(measurement=table_name))
    if len(result_point)>0:
        DBclient.query('DROP MEASUREMENT '+table_name)
    
