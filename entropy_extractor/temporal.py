import tensorflow as tf
import cv2
import numpy as np
import time
from conv import SimpleConv
from entropy import conv_entropy
gpus = tf.config.experimental.list_physical_devices('GPU')
tf.config.experimental.set_virtual_device_configuration(gpus[1], [
  tf.config.experimental.VirtualDeviceConfiguration(memory_limit=2048)
])

simpleConv = SimpleConv()
def get_temp_conv_entropy(input_file, shot_list, return_value):
    stime = time.time()
    cap = cv2.VideoCapture(input_file)
    frame_count=0
    frame_sequence = []
    sample_frame_count = 0
    total_entropy = 0
    shot_list_idx = 0

    while True:
        ret, frame = cap.read()
        if ret is False:
            break
        if frame_count > shot_list[shot_list_idx][1]: 
            shot_list_idx+=1

        frame = tf.image.resize(frame, [512,512], method='bilinear')/255.0
        if frame_count%24==0 and shot_list[shot_list_idx][0] == 1:
            if sample_frame_count == 5:
                frame_sequence_tensor = np.stack(frame_sequence, axis=0)
                temp_conv_feature = simpleConv(frame_sequence_tensor)
                for channel in range(4):
                    signal = tf.keras.backend.flatten(temp_conv_feature[:,:,:,channel]).numpy().round(decimals=3)
                    total_entropy += conv_entropy(signal)
                sample_frame_count = 0; temp_conv_feature=[]; frame_sequence=[]
                
            else:
                frame_sequence.append(frame) 
                sample_frame_count += 1 
            print(frame_count)
        if frame_count>=5094:
            break
        
        frame_count += 1
        
    return_value.value = total_entropy
    print("temp done: %.4f "%(time.time()-stime), input_file)

# if __name__=="__main__":
    # s = time.time()
    # input_path = '/home/min/background_LiteOn_P1_2019-11-12_15:00:36.mp4'
    # total_entropy = get_temp_conv_entropy(input_path)
    
