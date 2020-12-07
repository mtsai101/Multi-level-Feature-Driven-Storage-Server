import os
import time
import numpy as np
from ffmpeg_popen import get_video_size, start_ffmpeg_process_in, start_ffmpeg_process_out, read_frame, write_frame, output_set
from functools import partial
import matplotlib.pyplot as plt
import cv2

# input_path = "/home/min/Pole1_2020-11-04_16-00-01.mp4"
# file_output = "/home/min/background_Pole1_2020-11-04_16-00-01.mp4"

# cols, rows = get_video_size(input_path)

lr = 0.05
check_res = True
rows = 1536
cols = 2048

def ProcVid1(proc_frame, input_path, output_path):
    global rows, cols
    process_out = start_ffmpeg_process_out(output_path, output_set, cols, rows)


    cap = cv2.VideoCapture(input_path)
    
    while True:
        ret, in_frame = cap.read()
        if ret:
            raw_frame_rgb = np.dsplit(in_frame, 3)
            proc_frame.frame.array_r[:] = raw_frame_rgb[2][:]; proc_frame.frame.array_g[:] = raw_frame_rgb[1][:]; proc_frame.frame.array_b[:] = raw_frame_rgb[0][:]
            proc_frame.ProcessFrame(lr)
            write_frame(process_out, proc_frame.res_frame)
        else:
            break

    process_out.stdin.close()
    process_out.wait()
    


class PinnedMem(object):
    def __init__(self, size, dtype=np.uint8):
        self.array_r = np.empty(size,dtype)
        self.array_g = np.empty(size,dtype)
        self.array_b = np.empty(size,dtype)
        cv2.cuda.registerPageLocked(self.array_r)
        cv2.cuda.registerPageLocked(self.array_g)
        cv2.cuda.registerPageLocked(self.array_b)
        self.pinned = True
        
    def __del__(self):
        cv2.cuda.unregisterPageLocked(self.array_r)
        cv2.cuda.unregisterPageLocked(self.array_g)
        cv2.cuda.unregisterPageLocked(self.array_b)
        self.pinned = False
    def __repr__(self):
        return f'pinned = {self.pinned}'


class ProcFrameCuda3:
    def __init__(self):
        self.rows = 1536
        self.cols = 2048
        self.bgmog2 = cv2.cuda.createBackgroundSubtractorMOG()
        self.stream = cv2.cuda_Stream()

        self.frame = PinnedMem((rows,cols,1))


        self.frame_device = cv2.cuda_GpuMat(rows,cols,cv2.CV_8UC3)
        self.frame_device_r = cv2.cuda_GpuMat(rows,cols,cv2.CV_8UC1)
        self.frame_device_g = cv2.cuda_GpuMat(rows,cols,cv2.CV_8UC1)
        self.frame_device_b = cv2.cuda_GpuMat(rows,cols,cv2.CV_8UC1)

        self.frame_device_fg_r = cv2.cuda_GpuMat(rows,cols,cv2.CV_8UC1)
        self.frame_device_fg_g = cv2.cuda_GpuMat(rows,cols,cv2.CV_8UC1)
        self.frame_device_fg_b = cv2.cuda_GpuMat(rows,cols,cv2.CV_8UC1)

        self.frame_device_mask = cv2.cuda_GpuMat(rows,cols,cv2.CV_8UC1)


        self.fg_host = PinnedMem((rows,cols,1))
        
    def ProcessFrame(self,lr):

        self.frame_device.upload(np.concatenate([self.frame.array_r, self.frame.array_g, self.frame.array_b], axis=2), self.stream)
        self.frame_device_r.upload(self.frame.array_r,self.stream) 
        self.frame_device_g.upload(self.frame.array_g,self.stream)
        self.frame_device_b.upload(self.frame.array_b,self.stream)

        self.bgmog2.apply(self.frame_device, lr,self.stream, self.frame_device_mask)
        cv2.cuda.bitwise_and(self.frame_device_r, self.frame_device_r, self.frame_device_fg_r, self.frame_device_mask, self.stream)
        cv2.cuda.bitwise_and(self.frame_device_g, self.frame_device_g, self.frame_device_fg_g, self.frame_device_mask, self.stream)
        cv2.cuda.bitwise_and(self.frame_device_b, self.frame_device_b, self.frame_device_fg_b, self.frame_device_mask, self.stream)

        self.frame_device_fg_r.download(self.stream,self.fg_host.array_r)   
        self.frame_device_fg_g.download(self.stream,self.fg_host.array_g)           
        self.frame_device_fg_b.download(self.stream,self.fg_host.array_b)
        
        self.frame_device_fg_r.setTo(0, self.stream)
        self.frame_device_fg_g.setTo(0, self.stream)
        self.frame_device_fg_b.setTo(0, self.stream)
        self.stream.waitForCompletion() 

        self.res_frame = np.concatenate(
                    [
                        self.fg_host.array_r, 
                        self.fg_host.array_g, 
                        self.fg_host.array_b
                    ], axis=2
                )




if __name__=="__main__":

    input_path = "/home/min/Analytic-Aware_Storage_Server/storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-12_15:00:36.mp4"
    output_path = "/home/min/Analytic-Aware_Storage_Server/storage_server_volume/raw_videos/raw_11_9/ipcam1/background/background_LiteOn_P1_2019-11-12_15:00:36.mp4"
    proc_frame_cuda3 = ProcFrameCuda3()
    ProcVid1(proc_frame_cuda3, input_path, output_path)
    # print(f'GPU 3 (overlap host and device - attempt 1): {n_frames} frames, {gpu_time_3:.2f} ms/frame')

