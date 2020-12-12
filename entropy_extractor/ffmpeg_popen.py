# from PIL import Image
import numpy as np
import time 
import math
import ffmpeg
# from matplotlib import pyplot as plt
import sys
import cv2

import subprocess

import tensorflow as tf

# from tensorflow.image import ResizeMethod 
from tensorflow.keras import datasets, layers, models
import matplotlib.pyplot as plt
input_set={
        # "hwaccel":'cuda',
        # "hwaccel_output_format":'cuda'
        "hwaccel":'cuvid',
        "hwaccel_output_format":'hevc_cuvid'
    }
output_set={
        "c:v":'hevc_nvenc',
        "pix_fmt":'yuv420p',
        "b:v":'1M'
    }

def get_video_size(filename):
    # print('Getting video size for {!r}'.format(filename))
    # probe = ffmpeg.probe(filename)
    # video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
    # width = int(video_info['width'])
    # height = int(video_info['height'])
    width = 2048
    height = 1536
    return width, height

def start_ffmpeg_process_in(in_filename):
    print('Starting ffmpeg process in')
    args = (
        ffmpeg
        .input(in_filename)
        .output('pipe:', format='rawvideo', pix_fmt='rgb24')
        .compile()
    )
    return subprocess.Popen(args, stdout=subprocess.PIPE)


def start_ffmpeg_process_out(out_filename, output_set, width, height):
    # print('Starting ffmpeg process out')
    # print(width,height)
    args = (
        ffmpeg
        .input('pipe:', format='rawvideo', pix_fmt='rgb24', s='{}x{}'.format(width, height))
        .output(out_filename, **output_set)
        .overwrite_output()
        .compile()
    )
    return subprocess.Popen(args, stdin=subprocess.PIPE)

def read_frame(process_in, width, height):
    # print('Reading frame')
    # Note: RGB24 == 3 bytes per pixel.
    frame_size = width * height * 3
    in_bytes = process_in.stdout.read(frame_size)

    if len(in_bytes) == 0:
        frame = None
        ret = False
    else:
        assert len(in_bytes) == frame_size
        frame = (
            np
            .frombuffer(in_bytes, np.uint8)
            .reshape([height, width, 3])
        )
        ret = True
    return ret, frame

def write_frame(process_out, frame):
    # print('Writing frame')
    process_out.stdin.write(
        frame.astype(np.uint8).tobytes()
    )
 
if __name__=="__main__":
    displayResize = 600
    minArea = 6000
    fgbg_knn = cv2.createBackgroundSubtractorKNN(detectShadows=True)

    input_path = "/home/min/LiteOn_P1_2019-11-12_15:00:36.mp4"
    file_output = "/home/min/background_LiteOn_P1_2019-11-12_15:00:36.mp4"

    width, height = get_video_size(input_path)


    process_in = start_ffmpeg_process_in(input_path)
    process_out = start_ffmpeg_process_out(file_output, output_set, width, height)

    while True:
        in_frame = read_frame(process_in, width, height)

        if in_frame is None:
            break

        out_frame = in_frame
        write_frame(process_out, out_frame)

    process_in.wait()
    process_out.stdin.close()
    process_out.wait()


