import cv2
import numpy as np
from influxdb import InfluxDBClient
DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')
import time
class ShotDetector():
    def __init__(self, threshold=12, min_percent=0.999, min_scene_len=48, block_size=8):
        """Initializes threshold-based scene detector object."""
        self.threshold = int(threshold)
        self.min_percent = min_percent
        self.min_scene_len = min_scene_len
        self.processed_frame = False
        self.last_cut = 0
        self.block_size = block_size
        self.shot_list = [] # frame number where the last detected fade is, type of fade, can be either 1: 'over' or 0: 'below'
        self.frame_num = 0
    def frame_under_threshold(self, frame):
        num_pixel_values = float(frame.shape[0] * frame.shape[1] * frame.shape[2])
        large_ratio = self.min_percent > 0.5
        ratio = 1.0 - self.min_percent if large_ratio else self.min_percent
        min_pixels = int(num_pixel_values * ratio)

        self.curr_frame_amt = 0
        curr_frame_row = 0

        while curr_frame_row < frame.shape[0]:
            block = frame[curr_frame_row : curr_frame_row + self.block_size, :, :]
            if large_ratio:
                self.curr_frame_amt += int(np.sum(block > self.threshold))
            else:
                self.curr_frame_amt += int(np.sum(block <= self.threshold))
            
            if self.curr_frame_amt > min_pixels:
                return not large_ratio
            curr_frame_row += self.block_size
        return large_ratio

    def process_frame(self, frame_img):
        under_th = self.frame_under_threshold(frame_img) % 2
        if self.processed_frame:
            if under_th ^ self.shot_list[-1][0]:
                if(self.frame_num - self.last_cut)>=self.min_scene_len: ## 0/1 shot should longer than 48 frames
                    self.shot_list.append([ under_th % 2, self.frame_num-1])
                    self.last_cut = self.frame_num
                elif len(self.shot_list)==1:
                    self.shot_list[0][0] = (self.shot_list[0][0] + 1) % 2
                else:
                    self.shot_list.pop()
                    self.last_cut = self.shot_list[-1][1]
        else:           
            self.shot_list.append([under_th, 0])
            self.last_cut = 0


        self.processed_frame = True
        self.frame_num += 1

    def post_process(self):
        if self.frame_num - self.last_cut <= self.min_scene_len:
            self.shot_list[-1][1] = self.frame_num-1
        else:
            self.shot_list.append([ (self.shot_list[-1][0]+1)%2, self.frame_num-1] )
        if len(self.shot_list)>1:
            del self.shot_list[0]

    def detect(self, input_path): # the input_path here is background subtraction already
        cap = cv2.VideoCapture(input_path)
        count = 0
        while True:
            ret, frame = cap.read()
            if ret is False:
                print(self.frame_num)
                break
            self.process_frame(frame)
            
        self.post_process()


    def save_results(self, input_path): # the input_path here is the raw video
        json_body = [
                {
                    "measurement": "shot_list",
                    "tags": {
                        "name": str(input_path)
                    },
                    "fields": {
                        "list": str(self.shot_list)
                    }
                }
            ]
        DBclient.write_points(json_body)

if __name__=="__main__":
    # s = time.time()
    # shotDetector = ShotDetector()
    # input_path = "/home/min/Analytic-Aware_Storage_Server/storage_server_volume/raw_videos/raw_11_9/ipcam1/background/background_LiteOn_P1_2019-11-10_15:20:05.mp4"
    # shotDetector.detect(input_path)
    # shotDetector.save_results("/home/min/Analytic-Aware_Storage_Server/storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-10_15:20:05.mp4")
    # print(shotDetector.shot_list, time.time()-s)
    # del shotDetector
    s = time.time()
    shotDetector = ShotDetector()
    input_path="/home/min/Analytic-Aware_Storage_Server/storage_server_volume/raw_videos/raw_11_9/ipcam1/background/background_LiteOn_P1_2019-11-12_15:00:36.mp4"
    shotDetector.detect(input_path)
    shotDetector.save_results("/home/min/Analytic-Aware_Storage_Server/storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-12_15:00:36.mp4")
    print(shotDetector.shot_list, time.time()-s)
    del shotDetector
    
