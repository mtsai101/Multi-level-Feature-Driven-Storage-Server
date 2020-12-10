from influxdb import InfluxDBClient
DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')

import datetime
import pandas as pd
import numpy as np
import time
ANALY_LIST=['illegal_parking0','people_counting']
DOWN_LIST=['temporal','bitrate','qp']

pre_a_selected=[5.0, 10.0, 25.0, 50.0, 100.0]
pre_frame_rate = [24.0,12.0,6.0,1.0]
pre_bitrate = [1000.0, 500.0, 100.0, 10.0]
pre_d_selected = []
for f in pre_frame_rate:
    for b in pre_bitrate:
        pre_d_selected.append([f,b])
pre_d_selected=np.array(pre_d_selected) 



class Table:
    def __init__(self,table_name):
        self.table_name = table_name
        self.warm_up = 100
        self.last_update_time = 0
        self.window_size = 12
        self.result_point = list()
        self.analy_result_table = None
        self.down_result_table = None

        

    def get_latest_value(self, day_idx, time_idx, a_type='None', a_parameter=-1, fps=24.0, bitrate=1000.0):
        # day_idx, time_idx = get_context(clip_name)
        """
        step1 : check the very time is in weekday or weekend
        step2 : check nearest, switch to another weektime every time slot.
        """
        for r in range(0,11): # 
            for w in range(2):# check the corresponding time in weekend
                for i in range(2*r,2*r+2): # check neighbor which time difference is +1 or -1 
                    step = int(i/2) * pow((-1),i)
                    new_time_idx = (time_idx + step + self.window_size) % self.window_size
                    
                    target_col, estimation_point = self.make_query(day_idx,new_time_idx,a_type,a_parameter,fps, bitrate)
                    if estimation_point.shape[0]==self.window_size:
                        estimation = estimation_point.loc[:,target_col].mean()
                        return estimation
                    
                day_idx = (day_idx+1) % 2
        return 0.0

    def make_query(self,day_idx, time_idx, a_type, a_parameter, fps, bitrate):

        if self.model_type == 'IA':
            target_col = 'info_amount'
            if a_parameter != -1.0: # For \hat{e}(c,a,l)
                result = DBclient.query('SELECT target/a_parameter AS info_amount FROM '+self.table_name+' WHERE \"a_type\"=\''+ a_type +'\' AND \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx) + '\' AND a_parameter=' + str(a_parameter) + ' ORDER BY time DESC LIMIT ' + str(self.window_size))
                estimation_point = pd.DataFrame(result.get_points(measurement=self.table_name))
            else: # For \hat{e}(c,a,d)
                result = DBclient.query('SELECT target/a_parameter AS info_amount FROM down_result_analy WHERE \"a_type\"=\''+ a_type +'\' AND \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx) + '\' AND \"fps\"=\''+ str(fps) + '\' AND \"bitrate\"=\'' + str(bitrate) + '\' ORDER BY time DESC LIMIT ' + str(self.window_size))
                estimation_point = pd.DataFrame(result.get_points(measurement="down_result_analy"))

        elif self.model_type == 'AnalyTime':
            target_col = 'time_consumption'
            # For t(c,a,l)
            result = DBclient.query('SELECT * FROM '+self.table_name+' WHERE \"a_type\"=\'' + a_type + '\' AND \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx) + '\' AND a_parameter='+str(a_parameter) +' ORDER BY time DESC LIMIT ' + str(self.window_size))
            estimation_point = pd.DataFrame(result.get_points(measurement=self.table_name))
        elif self.model_type == 'DownTime':
            target_col = 'time_consumption'
            ## if fps and bitrate equal to default, return time = 0 (no downsample)
            if fps == pre_frame_rate[0] and bitrate == pre_bitrate[0]:
                estimation_point = pd.DataFrame(0, index=np.arange(self.window_size), columns=[target_col])
                return target_col, estimation_point

            result = DBclient.query('SELECT * FROM '+self.table_name+' WHERE \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx) + '\' AND \"fps\"=\''+str(fps) +'\' AND \"bitrate\"=\''+str(bitrate)+'\' ORDER BY time DESC LIMIT ' + str(self.window_size))
            estimation_point = pd.DataFrame(result.get_points(measurement=self.table_name))
        elif self.model_type == 'DownRatio':
            target_col = 'ratio'
            ## if fps and bitrate equal to default, return ratio = 1 (no downsample)
            if fps == pre_frame_rate[0] and bitrate == pre_bitrate[0]:
                estimation_point = pd.DataFrame(1, index=np.arange(self.window_size), columns=[target_col])
                return target_col, estimation_point
            result = DBclient.query('SELECT * FROM '+self.table_name+' WHERE \"day_of_week\"=\'' + str(day_idx) + '\' AND \"time_of_day\"=\'' + str(time_idx) + '\' AND \"fps\"=\'' + str(fps) +'\' AND \"bitrate\"=\'' + str(bitrate) + '\' ORDER BY time DESC LIMIT ' + str(self.window_size))
            estimation_point = pd.DataFrame(result.get_points(measurement=self.table_name))
        else:
            raise NameError('Unknown Query Name')

        
        
        if target_col == 'info_amount' and not estimation_point.empty:
            estimation_point['info_amount'] = estimation_point['info_amount']/(self.max_info[a_type])

        return target_col, estimation_point
    
class AnalyTimeTable(Table):
    def __init__(self,refresh):
        super().__init__('analy_result')
        self.model_type='AnalyTime'
        self.refresh = refresh
        if self.refresh:
            print("[INFO] Updating the AnalyTimeTable models...")

            drop_measurement_if_exist("AnalyTimeTable")

            for day_idx in range(2):
                for time_idx in range(12):
                    for a_type in ANALY_LIST:
                        for a_parameter in pre_a_selected:
                            value = self.get_latest_value(day_idx, time_idx, a_type=a_type, a_parameter=a_parameter)
                            json_body = [
                                    {
                                        "measurement":self.model_type + 'Table',
                                        "tags": {
                                            "name": self.model_type,
                                            "a_type":a_type,
                                            "a_parameter":float(a_parameter),
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
        else:
            result = DBclient.query('SELECT * FROM AnalyTimeTable')
            result_point = list(result.get_points(measurement='AnalyTimeTable'))

            self.table = np.zeros(len(result_point), dtype = np.float32)

            for k,i in enumerate(result_point):
                self.table[k] =  i["value"]

            self.table = self.table.reshape((2,12,2,len(pre_a_selected)))

    def get_estimation(self,day_of_week=0,time_of_day=0,a_type=-1,a_parameter=-1):
        return self.table[day_of_week][time_of_day][a_type][a_parameter]
        

class DownTimeTable(Table):
    def __init__(self,refresh):
        super().__init__('down_result')
        self.model_type='DownTime'
        self.refresh = refresh
        if self.refresh:
            print("[INFO] Updating the DownTimeTable models...")
            drop_measurement_if_exist("DownTimeTable")

            for day_idx in range(2):
                for time_idx in range(12):
                    for d_parameter in pre_d_selected:
                        value = self.get_latest_value(day_idx, time_idx, fps=d_parameter[0], bitrate=d_parameter[1])
                        json_body = [
                                {
                                    "measurement":self.model_type + 'PredTable',
                                    "tags": {
                                        "name": self.model_type,
                                        "fps":float(d_parameter[0]),
                                        "bitrate":float(d_parameter[1]),
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
        else:
            result = DBclient.query('SELECT * FROM DownTimePredTable')
            result_point = list(result.get_points(measurement='DownTimePredTable'))

            self.table = np.zeros(len(result_point), dtype = np.float32)

            for k,i in enumerate(result_point):
                self.table[k] =  i["value"]
            self.table = self.table.reshape((2,12,pre_d_selected.shape[0]))

    def get_estimation(self, day_of_week=0, time_of_day=0, p_id=-1):
        if p_id==0 or p_id==-1:
            return 0
        else:
            return self.table[day_of_week][time_of_day][p_id]

                            
class DownRatioTable(Table):
    def __init__(self,refresh):
        super().__init__('down_result')
        self.model_type='DownRatio'
        self.refresh =refresh
        if self.refresh:
            print("[INFO] Updating the DownRatioTable models...")
            drop_measurement_if_exist("DownRatioTable")
            for day_idx in range(2):
                for time_idx in range(12):
                    for d_parameter in pre_d_selected:
                        value = self.get_latest_value(day_idx, time_idx, fps=d_parameter[0], bitrate=d_parameter[1])
                        json_body = [
                                {
                                    "measurement":self.model_type + 'PredTable',
                                    "tags": {
                                        "name": self.model_type,
                                        "fps":d_parameter[0],
                                        "bitrate":d_parameter[1],
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
        else:
            result = DBclient.query('SELECT * FROM DownRatioPredTable')
            result_point = list(result.get_points(measurement='DownRatioPredTable'))

            self.table = np.zeros(len(result_point), dtype = np.float32)

            for k,i in enumerate(result_point):
                self.table[k] =  i["value"]
            self.table = self.table.reshape((2,12,pre_d_selected.shape[0]))

    def get_estimation(self, day_of_week=0, time_of_day=0,p_id=-1):
        if p_id==-1:
            return 0
        elif p_id == 0:
            return 1
        else:
            return self.table[day_of_week][time_of_day][p_id]

        
class IATable(Table):
    def __init__(self,refresh):
        super().__init__('analy_result')

        self.model_type='IA'
        self.max_info=dict()
        self.refresh = refresh
         # store max_value
        if self.refresh:
            for a_type in ANALY_LIST:
                max_result = DBclient.query('SELECT target/a_parameter AS info_amount FROM ' + self.table_name + ' WHERE a_type=\''+a_type+'\'')
                max_result = list(max_result.get_points(measurement=self.table_name))
                max_value = sorted(max_result, key=lambda k :k['info_amount'],reverse=True)[0]['info_amount']
                self.max_info[a_type] = max_value
            ## influxdb can not update, only we can do is deleting the previous one
            print("[INFO] Updating the IATable models...")
            drop_measurement_if_exist("IAPredTable")
            for day_idx in range(2):
                for time_idx in range(12):
                    for a_type in ANALY_LIST:
                        for a_parameter in pre_a_selected:
                            value = self.get_latest_value(day_idx, time_idx, a_type=a_type, a_parameter=a_parameter)
                            json_body = [
                                    {
                                        "measurement":self.model_type + 'PredTable',
                                        "tags": {
                                            "name": self.model_type,
                                            "a_type":a_type,
                                            "a_parameter":float(a_parameter),
                                            "fps":float(pre_frame_rate[0]),
                                            "bitrate":float(pre_bitrate[0]),
                                            "day_of_week":day_idx,
                                            "time_of_day":time_idx,
                                        },
                                        "fields": {
                                            "value":value
                                        }
                                    }
                                ]
                            DBclient.write_points(json_body)
                        for d_parameter in pre_d_selected:
                            value = self.get_latest_value(day_idx, time_idx, a_type=a_type, fps=d_parameter[0],bitrate=d_parameter[1])
                            # value = -1.0
                            json_body = [
                                    {
                                        "measurement":self.model_type + 'PredTable',
                                        "tags": {
                                            "name": self.model_type,
                                            "a_type":a_type,
                                            "a_parameter":float(-1),
                                            "fps":float(d_parameter[0]),
                                            "bitrate":float(d_parameter[1]),
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
        
        result = DBclient.query('SELECT * FROM IAPredTable')
        result_point = list(result.get_points(measurement='IAPredTable'))
        self.table = np.zeros(len(result_point), dtype = np.float32)

        for k,i in enumerate(result_point):
            self.table[k] =  i["value"]

        self.table = self.table.reshape((2,12,len(ANALY_LIST),len(pre_a_selected)+len(pre_d_selected)))
        

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
    
