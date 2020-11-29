import os
import time
import numpy as np
from ffmpeg_popen import get_video_size, start_ffmpeg_process_in, start_ffmpeg_process_out, read_frame, write_frame, output_set
import ffmpeg
from functools import partial
import cv2
import sys
# input_path = "/home/min/Analytic-Aware_Storage_Server/storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-12_15:00:36.mp4"
# file_output = "/home/min/Analytic-Aware_Storage_Server/storage_server_volume/raw_videos/raw_11_9/ipcam1/background/background_LiteOn_P1_2019-11-12_15:00:36.mp4"



lr = 0.05
check_res = True

def ProcVid1(proc_frame, lr, cols, rows, input_path, output_path):

    process_out = start_ffmpeg_process_out(output_path, output_set, cols, rows)

    n_frames = 0
    start = time.time()
    cap = cv2.VideoCapture(input_path)

    while True:
        ret, in_frame = cap.read()
        if ret:
            n_frames += 1
            raw_frame_rgb = np.dsplit(in_frame, 3)
            proc_frame.frame.array_r[:] = raw_frame_rgb[2][:]; proc_frame.frame.array_g[:] = raw_frame_rgb[1][:]; proc_frame.frame.array_b[:] = raw_frame_rgb[0][:]
            proc_frame.ProcessFrame(lr)
            write_frame(process_out, proc_frame.res_frame)
        else:
            break

    process_out.stdin.close()
    process_out.wait()
    

    end = time.time()
    return (end - start)*1000/n_frames, n_frames

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

class PinnedMem_test(object):
    def __init__(self, size, dtype=np.uint8):
        self.array = np.empty(size,dtype)
        cv2.cuda.registerPageLocked(self.array)
        self.pinned = True
        
    def __del__(self):
        cv2.cuda.unregisterPageLocked(self.array)
        self.pinned = False
    def __repr__(self):
        return f'pinned = {self.pinned}'


class ProcFrameCuda3:
    def __init__(self, rows, cols, store_res=False):
        self.rows, self.cols = rows, cols
        self.store_res = store_res
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
    cols, rows = get_video_size("")
    proc_frame_cuda3 = ProcFrameCuda3(rows,cols,check_res)
    # some modify from chopper
    for i in range(4,10):
        dir_path = "/mnt/data6/minhan/SmartPole/Pole1/2020-11-0"+str(i)+"_00-00-00"

        vli_back_dir = os.path.join(dir_path,"background")
        if not os.path.isdir(vli_back_dir):
            os.mkdir(vli_back_dir)

        # remove broken video
        vli = os.listdir(dir_path)
        for v in vli:
            input_path = os.path.join(dir_path, v)
            if not os.path.isfile(input_path):
                continue
            try:
                probe = ffmpeg.probe(input_path)
            except Exception as e:
                broken_hour = input_path.split("/")[-1].split("_")[-1].split("-")[0]
                broken_day = input_path.split("/")[-1].split("_")[1].split("-")[-1]
                rm_cmd = "rm " + os.path.join(dir_path, "Pole1_2020-11-") + broken_day + "_" + broken_hour+"*"
                os.system(rm_cmd)

        # process good video
        vli = os.listdir(dir_path)
        for v in vli:
            input_path = os.path.join(dir_path, v)
            output_path = os.path.join(vli_back_dir, "background_"+v)
            if os.path.isfile(input_path) and not os.path.isfile(output_path):
                gpu_time_3, n_frames = ProcVid1(proc_frame_cuda3, lr, rows, cols, input_path, output_path)
                    
