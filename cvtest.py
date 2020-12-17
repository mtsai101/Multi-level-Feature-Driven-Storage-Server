import cv2
import time
cap = cv2.VideoCapture("/home/min/SmartPole/Pole1/2020-11-10_00-00-00/Pole1_2020-11-10_12-00-01.mp4")
while True:
    s = time.time()
    ret, frame = cap.read()
    if ret is False:
        break
    print(1/(time.time()-s))
