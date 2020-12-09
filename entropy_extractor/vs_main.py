from color import get_color_entropy
from edge import get_edge_entropy
from conv import get_conv_entropy
from temporal import get_temp_conv_entropy
from influxdb import InfluxDBClient
from CUDA_background import ProcVid1, ProcFrameCuda3
from shot_detection import ShotDetector
import multiprocessing as mp
import time
import os 
import concurrent.futures
import ast 
DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')


# mp.set_start_method('spawn') # This is important for multiprocessing CUDA
def background_subtraction(pending_tuple):
    proc_frame_cuda3 = ProcFrameCuda3()
    ProcVid1(proc_frame_cuda3, pending_tuple[0], pending_tuple[1])
    print(pending_tuple[0])
    # del proc_frame_cuda3

def launch_shot_detector(pending_tuple):
    s = time.time()
    shotDetector = ShotDetector()
    shotDetector.detect(pending_tuple[1])
    shotDetector.save_results(pending_tuple[0])
    
    print("Finst detect the shots of ", pending_tuple[1])
    print("Total %d frames, take %f sec"%(shotDetector.frame_num, time.time()-s))
    del shotDetector

def feature_procs(input_path, pending_tuple):
    
    # s = time.time()
    result = DBclient.query("SELECT * FROM shot_list where \"name\"=\'"+pending_tuple[0]+"\'")
    shot_list = list(result.get_points(measurement='shot_list'))[0]['list']
    shot_list = ast.literal_eval(shot_list) # convert str representation of list to list

    # color_entropy = mp.Value('d', 0.0); 
    edge_entropy = mp.Value('d', 0.0); #conv_entropy = mp.Value('d', 0.0); temp_entropy = mp.Value('d', 0.0)
    # color_proc = mp.Process(target=get_color_entropy, args=(pending_tuple[1], shot_list, color_entropy,))
    edge_proc = mp.Process(target=get_edge_entropy, args=(input_path, , shot_list, edge_entropy,))
    # # conv_proc = mp.Process(target=get_conv_entropy, args=(input_path, conv_entropy,))
    # # temp_proc = mp.Process(target=get_temp_conv_entropy, args=(input_path, temp_entropy,))
    # color_proc.start(); 
    edge_proc.start(); #conv_proc.start(); temp_proc.start()
    # color_proc.join(); 
    edge_proc.join(); #conv_proc.join(); temp_proc.join()

    # print("color: %f, edge: %f, conv: %f, temp: %f"%(color_entropy.value,edge_entropy.value,conv_entropy.value,temp_entropy.value))
    
    # json_body = [
    #             {
    #                 "measurement": "visual_features_entropy_unnormalized",
    #                 "tags": {
    #                     "name": str(input_path)
    #                 },
    #                 "fields": {
    #                     "color": float(color_entropy.value),
    #                     "edge": float(edge_entropy.value),
    #                     "conv": float(conv_entropy.value),
    #                     "temp": float(temp_entropy.value)
    #                 }
    #             }
    #         ]
    # DBclient.write_points(json_body)

if __name__=="__main__":
    feature_pending_list = [] # (input_path, back_path)
    month = 11; day=4
    month = str(month) if month>9 else "0"+str(month)
    day = str(day) if day>9 else "0"+str(day)
    input_dir = "/home/min/Analytic-Aware_Storage_Server/storage_server_volume/SmartPole/Pole1/2020-"+str(month)+"-"+str(day)+"_00-00-00"
    v_li = os.listdir(input_dir)

    for v in v_li:
        input_path = os.path.join(input_dir, v)
        output_dir = os.path.join(input_dir,"background")
        if not os.path.isfile(input_path):
            continue
        if not os.path.isdir(os.path.join(input_dir,"background")):
            os.mkdir(os.path.join(input_dir,"background"))

        back_path = os.path.join(output_dir,"background_"+v)
        feature_pending_list.append((input_path, back_path)) 

        
    if the video already be background subtraction, leave it be commented
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(background_subtraction, feature_pending_list)

    print("Background Subtraction Completed")



    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(launch_shot_detector, feature_pending_list)
    print("Shot Detection Completed")
        
    
    feature_procs(input_path, feature_pending_list[0])
    print("Feature Extraction Completed")
