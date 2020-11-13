from .illegal_parking_detector import ParkingDetector

from PIL import Image

from influxdb import InfluxDBClient

# use YOLO by darknet
from .darknet import darknet

from optimal_downsampling_manager.resource_predictor.table_estimator import get_context
import numpy as np

import argparse
import warnings
import pickle
import random
import time
import math
import sys
import cv2
import csv
import os


class Analyst(object):
    def __init__(self):
        self.current_sample_rate = 0
        self.framesCounter = 0
        self.netMain = None 
        self.metaMain = None
        self.altNames = None
        self.darknet_image = None

        self.park_poly_list=list()
        self.processing_time = 0.0
        self.vs = None
        self.fps = 0.0
        self.write2disk = False
        self.writer = None
        self.clip_name = None
        self.DBclient = InfluxDBClient('localhost', 8086, 'root', 'root', 'video_edge')
        self.busy = 0 
        self.target_counter = 0

    def set_net(self, netMain, metaMain, altNames):
        self.netMain = netMain
        self.metaMain = metaMain
        self.altNames = altNames
        self.darknet_image = darknet.make_image(darknet.network_width(self.netMain),darknet.network_height(self.netMain),3)
        
    def set_illegal_region(self,park_poly):
        park_poly = np.array([park_poly], dtype = np.int32)
        self.park_poly_list.append(park_poly)
   
    def save_result_video(self, w=False):
        self.write2disk = w
        
    def set_sample_rate(self,sample_rate):
        self.current_sample_rate = sample_rate

    def set_video_clip(self,clip_name):

        if self.vs is not None:
            self.vs.release()
        if self.writer is not None:
            self.writer.release()

        print("[INFO] opening video file...")

        self.vs = cv2.VideoCapture(clip_name)

        self.read_fps = self.vs.get(cv2.CAP_PROP_FPS)

        self.total_frame_num = self.vs.get(cv2.CAP_PROP_FRAME_COUNT)
        self.img_width = int(self.vs.get(cv2.CAP_PROP_FRAME_WIDTH))
        self.img_height = int(self.vs.get(cv2.CAP_PROP_FRAME_HEIGHT))

        self.processing_fps = 0.0
        self.writer = None

    def analyze_save_clean(self,S_decision):
        
        print("[INFO] Saving the analytic result")

        if(self.framesCounter>0):
            record_time = time.asctime(time.localtime(time.time()))
            #save info_amount by type

            day_idx, time_idx = get_context(S_decision.clip_name)

            json_body = [
                {
                    "measurement": "analy_result",
                    "tags": {
                        "name": str(S_decision.clip_name),
                        "a_type": str(S_decision.a_type),
                        "day_of_week":int(day_idx),
                        "time_of_day":int(time_idx),
                        "host": "webcamPole1"
                    },
                    "fields": {
                        "a_parameter": float(self.framesCounter),
                        "fps": float(S_decision.fps),
                        "bitrate": float(S_decision.bitrate),
                        "time_consumption": float(self.processing_time),
                        "target": int(self.target_counter)
                    }
                }
            ]

            self.DBclient.write_points(json_body)


        print("total processed {} frames".format(self.framesCounter))
        print("[INFO] Refresh the analtic type")
        
        # reset the counter
        self.target_counter = 0
        self.framesCounter = 0
        
        
    def analyze(self, clip_name, type_, sample_length):
        #set writer

        if self.writer is None and self.write2disk is True:
            fourcc = cv2.VideoWriter_fourcc(*'XVID')
            self.writer = cv2.VideoWriter(
                                './storage_server_volume/stored_video/' + clip_name.split('/')[-1],
                                fourcc,
                                self.read_fps,
                                (darknet.network_width(self.netMain), darknet.network_height(self.netMain)),
                                True
                            )
        
        
        start = time.time()
        chunk = self.total_frame_num/sample_length
        step = 1 # 1 step for 1 chunk
        framesCounter=-1
        s=time.time()

        
        while True:
            if self.framesCounter == sample_length: ## get enough number of frame
                break
            framesCounter += 1

            success = self.vs.grab()
            if success == False:
                break

            if sample_length!=-1:
                if framesCounter != int(chunk*step-chunk/2):
                    continue
                else:
                    step+=1

            ret, frame = self.vs.retrieve()
            
            if type_ == 'None':
                if self.write2disk:
                    self.writer.write(frame)
                    continue

            # execute yolo
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            image = cv2.resize(frame_rgb,
                                   (darknet.network_width(self.netMain),
                                    darknet.network_height(self.netMain)),
                                   interpolation=cv2.INTER_LINEAR)

            darknet.copy_image_from_bytes(self.darknet_image,image.tobytes())
            detections = darknet.detect_image(self.netMain, self.metaMain, self.darknet_image, thresh=0.75,debug=False)
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            boxs_ = []
            classes_ = []
            if type_[:-1] == 'illegal_parking':
                for obj in detections:
                    class_ = obj[0].decode("utf-8")
                    if class_=='car' or class_=='motorbike' or class_=='bus' or class_=='truck':
                        boxs_.append(list(obj[-1]))
                        classes_.append(class_)    
                if len(detections) > 0:
                    # loop over the indexes we are keeping
                    for det in detections:
                        bbox = det[-1]
                        # extract the bounding box coordinates
                        (x, y) = (int(bbox[0]), int(bbox[1]))
                        (w, h) = (int(bbox[2]-bbox[0]), int(bbox[3]-bbox[1]))
                        obj_park =np.array([[
                                        [x,y],
                                        [x+w,y],
                                        [x+w,y+h],
                                        [x,y+h]
                                    ]], dtype = np.int32)
                        
                        # draw a bounding box rectangle and label on the frame
                        
                        illegal_park, image, cover_ratio  = ParkingDetector.detect(image, self.park_poly_list[int(type_[-1])], obj_park)
                        if illegal_park == True:
                            self.target_counter+=1

                        text = "region {}: {}".format(type_[-1], illegal_park)
                        cv2.putText(image, text, (10, darknet.network_height(self.netMain)-30), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (0, 255, 0), 2)
                # check to see if we should write the frame to disk
                if self.write2disk:
                    self.writer.write(image)
                
               
            elif type_ == 'people_counting':
                for obj in detections:
                    class_ = obj[0].decode("utf-8")
                    if class_=='person':
                        boxs_.append(list(obj[-1]))

                self.target_counter += len(boxs_)
                
                
                
                if self.write2disk:
                    for bbox in boxs_:
                        cvDrawBoxes("person",bbox,image)

                    text = "Number of people: {}".format(len(boxs_))

                    cv2.putText(image, text, (10, darknet.network_height(self.netMain) - 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
                    # check to see if we should write the frame to disk
                    self.writer.write(image)

            self.framesCounter += 1   
            

        p_t = time.time()-start
        print(framesCounter)
        print(p_t)
        self.processing_fps = float(self.framesCounter)/p_t
        self.processing_time = p_t
        if self.writer is not None:
            self.writer.release()
        self.vs.release()

        
def convertBack(x, y, w, h):
    xmin = int(round(x - (w / 2)))
    xmax = int(round(x + (w / 2)))
    ymin = int(round(y - (h / 2)))
    ymax = int(round(y + (h / 2)))
    return xmin, ymin, xmax, ymax


def cvDrawBoxes(class_,bbox, img):
    x, y, w, h = bbox[0],bbox[1],bbox[2],bbox[3]
    xmin, ymin, xmax, ymax = convertBack(
        float(x), float(y), float(w), float(h))
    pt1 = (xmin, ymin)
    pt2 = (xmax, ymax)
    cv2.rectangle(img, pt1, pt2, (0, 255, 0), 1)
    cv2.putText(img,
                class_,
                (pt1[0], pt1[1] - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                [0, 255, 0], 2)
    return img


