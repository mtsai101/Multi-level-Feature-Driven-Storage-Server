from influxdb import InfluxDBClient
from optimal_downsampling_manager.resource_predictor.table_estimator import AnalyTimeTable, DownTimeTable, DownRatioTable, IATable

DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'video_edge')
if __name__=='__main__':
    # init raw video database
    # json_body = [
    #                     {
    #                         "measurement": "raw_11_9",
    #                         "tags": {
    #                             "name": "./storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-09_09:31:53.mp4"
                                
    #                         },
    #                         "fields": {
    #                             "host": "webcamPole1",
    #                             "status": "unprocessed"
    #                         }
    #                     }
    #                 ]
    
    # DBclient.write_points(json_body)

    # init analy result
    # json_body = [
    #     {
    #         "measurement": "analy_result",
    #         "tags": {
    #             "a_type": "people_counting",
    #             "name": "./storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-09_09:31:53.mp4",
    #             "host": "webcamPole1"
    #         },
    #         "fields": {
    #             "day_of_week":int(0),
    #             "time_of_day":int(4),
    #             "a_parameter": float(100.0),
    #             "fps": float(24.0),
    #             "bitrate": float(1000.0),
    #             "time_consumption": float(0.0),
    #             "target": float(0.0)
    #         }
    #     }
    # ]
    # DBclient.write_points(json_body)
    
    # # # init down_result
    # json_body = [
    #         {
    #             "measurement": "down_result",
    #             "tags": {
    #                 "name": "./storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-09_09:31:53.mp4",
    #                 "host": "webcamPole1",
    #             },
    #             "fields": {
    #                 "day_of_week":int(0),
    #                 "time_of_day":int(4),
    #                 "a_parameter": float(100.0),
    #                 "fps": float(24.0),
    #                 "bitrate": float(1000.0),
    #                 "time_consumption": float(0.0),
    #                 "origin_size":float(0.0),
    #                 "result_size":float(0.0),
    #                 "ratio":float(1.0)
    #             }
    #         }
    #     ]
    # DBclient.write_points(json_body)

    # iaTable = IATable(True)
    # analyTimeTable = AnalyTimeTable(True)
    # downRatioTable = DownRatioTable(True)
    # downTimeTable = DownTimeTable(True)

