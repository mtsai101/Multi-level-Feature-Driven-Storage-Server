from entropy import conv_entropy
import numpy as np
import time
import cv2

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '1'

def get_conv_entropy(input_file, shot_list, return_value):
    from conv_model import SimpleConv
    import tensorflow as tf

    simpleConv = SimpleConv()
    total_entropy = 0
    vs = cv2.VideoCapture(input_file)
    frame_count = 0
    shot_list_idx = 0

    while True:
        ret, frame = vs.read()
        if ret is False:
            break
        if frame_count > shot_list[shot_list_idx][1]: 
            shot_list_idx+=1

        if frame_count%24==0 and shot_list[shot_list_idx][0] == 1:
            frame = tf.image.resize(frame, [256,256], method='bilinear')
            frame = tf.expand_dims(frame, axis=0)/255.0
            conv_feature = simpleConv(frame)
            for channel in range(4):
                signal = tf.keras.backend.flatten(conv_feature[:,:,:,channel]).numpy().round(decimals=3)
                total_entropy += conv_entropy(signal)

        frame_count += 1
    
    return_value.value = total_entropy

def get_temp_conv_entropy(input_file, shot_list, return_value):
    from conv_model import SimpleConv
    import tensorflow as tf
    stime = time.time()
    tempConv = SimpleConv()
    cap = cv2.VideoCapture(input_file)
    frame_count = 0
    frame_sequence = []
    sample_frame_count = 0
    total_entropy = 0
    shot_list_idx = 0
    
    while True:
        ret, frame = cap.read()
        if ret is False:
            break
        if frame_count > shot_list[shot_list_idx][1]: 
            shot_list_idx+=1; sample_frame_count = 0; frame_sequence=[]

        if frame_count%24==0 and shot_list[shot_list_idx][0]:
            frame_resized = tf.image.resize(frame, [256,256], method='bilinear')/255.0
            frame_sequence.append(frame_resized) 
            sample_frame_count += 1
            s = time.time()
            if sample_frame_count == 5:
                frame_sequence_tensor = np.stack(frame_sequence, axis=0)
                temp_conv_feature = tempConv(frame_sequence_tensor)
                for channel in range(4):
                    signal = tf.keras.backend.flatten(temp_conv_feature[:,:,:,channel]).numpy().round(decimals=3)
                    total_entropy += conv_entropy(signal)
                sample_frame_count = 0; frame_sequence=[]
        frame_count += 1
        
    return_value.value = total_entropy
    print("conv done: %.4f "%(time.time()-stime), input_file)

# if __name__=="__main__":
#     DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
#     import ast
#     input_file = "../storage_server_volume/SmartPole/Pole1/2020-11-04_00-00-00/Pole1_2020-11-04_02-00-00.mp4"
    
#     result = DBclient.query("SELECT * FROM shot_list where \"name\"=\'"+input_file[1:]+"\'")

#     shot_list = ast.literal_eval(list(result.get_points(measurement='shot_list'))[0]['list'])
    
#     s = time.time()
#     return_value =0
#     total_entropy = get_temp_conv_entropy(input_file, shot_list,return_value)
#     print(time.time()-s)
#     print(return_value)