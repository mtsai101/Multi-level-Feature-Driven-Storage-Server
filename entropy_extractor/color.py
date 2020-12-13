from entropy import color_entropy
import cv2
import time
import numpy as np
import concurrent.futures

def get_color_entropy(input_path, shot_list, return_value):
    vs = cv2.VideoCapture(input_path)
    total_entropy = 0
    frame_count = 0
    shot_list_idx = 0

    while True:

        ret, frame = vs.read()
        if ret is False:
            break
        if frame_count > shot_list[shot_list_idx][1]: 
            shot_list_idx+=1
        if frame_count%24==0 and shot_list[shot_list_idx][0] == 1:
            
            frame_rgb = np.dsplit(frame, 3)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                entropy_list = {executor.submit(color_entropy, frame_single_channel): frame_single_channel for frame_single_channel in frame_rgb}
            
            for entropy in entropy_list:
                total_entropy += entropy.result()

        frame_count +=1
        

    return_value.value = total_entropy





# if __name__=="__main__":
#     input_file = "/home/min/background_LiteOn_P1_2019-11-12_15:00:36.mp4"
#     s = time.time()
#     entropy = get_color_entropy(input_file)
#     print(time.time()-s)
#     print(entropy)

