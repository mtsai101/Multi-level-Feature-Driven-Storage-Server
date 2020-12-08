#! /usr/bin/env python3
import cv2
import time

import pulsar
import yaml
import threading
import numpy as np
import sys
import os
from influxdb import InfluxDBClient
from datetime import datetime
from pulsar import MessageId
from pulsar.schema import *

with open('configuration_manager/config.yaml','r') as yamlfile:
    data = yaml.load(yamlfile,Loader=yaml.FullLoader)['saver']

# Schema for the topic 
class Frame(Record):
    timestamp = String()
    img = Bytes()

influx_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'storage')


if not os.path.isdir("./storage_server_volume"):
    os.mkdir("./storage_server_volume")
if not os.path.isdir("./storage_server_volume/raw_videos"):
    os.mkdir("./storage_server_volume/raw_videos")
if not os.path.isdir("./storage_server_volume/raw_videos/ipcam8"):
    os.mkdir("./storage_server_volume/raw_videos/ipcam8")

class VideoSaver(object):
    def __init__(self):
        
        client = pulsar.Client(data['pulsar_url'])
        self.reader = client.create_reader(
                        topic=data['topic'],  
                        start_message_id=MessageId.latest, 
                        receiver_queue_size=5000,
                        schema=AvroSchema(Frame)
                    )
        self.count = 0
        self.sample_length = data['sample_length']
        self.isstop = False

    def queryframe(self):

        while (not self.isstop):
            try:
                msg = self.reader.read_next()

                frame = cv2.imdecode(np.frombuffer(msg.value().img, np.uint8), -1)
                
                img_width, img_height = frame.shape[:2]

                # set writer
                if (self.count % self.sample_length) == 0:
                    now = datetime.now()
                    record_time = str(now.year)+'_'+str(now.month)+'_'+str(now.day)+"_"+str(now.hour)+':'+str(now.minute)+':'+str(now.second)

                    video_name = "./storage_server_volume/raw_videos/ipcam8/"+record_time+".mp4"
                    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
                    self.writer = cv2.VideoWriter(video_name, fourcc, 24, (img_height,img_width),True)
                    self.count = 0
                    print("create a new video")

                    json_body = [
                        {
                            "measurement": "raw_videos",
                            "tags": {
                                "name": "./storage_server_volume/raw_videos/ipcam8/" + record_time +".mp4",
                                "status":"unprocessed"
                                "host": 'webcamPole8'
                            },
                            "fields": {
                                "frame_num": self.sample_length,
                            }
                        }
                    ]
                
                    influx_client.write_points(json_body)

                self.writer.write(frame)
                self.conn_send2SLE.send(True)
                    print("Send signal to SLE")
                self.count = self.count + 1
            except Exception as e:
                print(e)
                pass


