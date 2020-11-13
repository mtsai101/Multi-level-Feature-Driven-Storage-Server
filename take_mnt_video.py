import os
from influxdb import InfluxDBClient
from multiprocessing import Process,Pool


def func(files):
    for f in files:
        origin_path = os.path.join('/mnt/LiteOn_Videos/LiteOn_P1',f)
        if f.startswith("LiteOn_P1_2019-11-") and os.path.isfile(origin_path):
            
            if f.startswith("LiteOn_P1_2019-11-0") is False:
                print(f)

                day = f.split("_")[-2].split("-")[-1]
                target_dir = "./dataSet/videos/raw_11_"+str(int(day))
                if not os.path.isdir(target_dir):
                    os.makedirs(target_dir)  
                target_path = "./dataSet/videos/raw_11_"+str(int(day))
                os.system("cp " + origin_path+ " " + target_path)
                
                json_body = [
                                {
                                    "measurement": "raw_11_"+str(int(day)),
                                    "tags": {
                                        "name": os.path.join(target_path,f)
                                    },
                                    "fields": {
                                        "status": "unprocessed",
                                        "host":'webcamPole1'
                                        
                                    }
                                }                                                                                                                                                                        
                            ]
                influx_client.write_points(json_body)

    
if __name__=='__main__':
    pool = Pool(4)
    ## move video from NAS to dataSet/video/raw
    influx_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'video_edge')
    print("taking video...")
    files = os.listdir("/mnt/LiteOn_Videos/LiteOn_P1")

    num = int(len(files)/4)

    proc1 = Process(target = func, args = (files[:num],)).start()
    proc2 = Process(target = func, args = (files[num:num*2],)).start()
    proc3 = Process(target = func, args = (files[num*2:num*3],)).start()
    proc4 = Process(target = func, args = (files[num*3:],)).start()


    