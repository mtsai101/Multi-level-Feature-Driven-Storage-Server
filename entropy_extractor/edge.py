import cv2
import numpy as np
import math
import time
from entropy import edge_entropy
import concurrent.futures
class EdgeHistogramComputer():
    def __init__(self, rows, cols):
        sqrt2 = math.sqrt(2)
        self.kernels = (np.matrix([[1,1],[-1,-1]]), \
                np.matrix([[1,-1],[1,-1]]),         \
                np.matrix([[sqrt2,0],[0,-sqrt2]]),  \
                np.matrix([[0,sqrt2],[-sqrt2,0]]),  \
                np.matrix([[2,-2],[-2,2]]))
        self.bins = [len(self.kernels)]
        self.range = [0,len(self.kernels)]
        self.rows = rows
        self.cols = cols
        self.prefix = "EDH"
        self.total_entropy = 0

    def hist(self, descriptor, dominantGradients, row, col, mask):
        hist = cv2.calcHist([dominantGradients], [0], mask, self.bins, self.range)
        hist = cv2.normalize(hist, None)
        hist = np.round(hist,4)
        descriptor += hist

    def compute(self, frame):
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        descriptor = np.zeros((len(self.kernels),1))
        dominantGradients = np.zeros_like(frame)
        maxGradient = cv2.filter2D(frame, cv2.CV_32F, self.kernels[0])
        maxGradient = np.absolute(maxGradient)
        
        for k in range(1,len(self.kernels)):
            kernel = self.kernels[k]
            gradient = cv2.filter2D(frame, cv2.CV_32F, kernel)
            gradient = np.absolute(gradient)
            np.maximum(maxGradient, gradient, maxGradient)
            indices = (maxGradient == gradient)
            dominantGradients[indices] = k

        frameH, frameW = frame.shape
        mask = np.zeros_like(frame)
        # for row in range(self.rows):
        #     for col in range(self.cols):
        #         mask[int((frameH/self.rows)*row):int((frameH/self.rows)*(row+1)),int((frameW/self.cols)*col):int((frameW/self.cols)*(col+1))] = 255
        #         # calcHist: (images, channels, mask, histSize, ranges)
        #         hist = cv2.calcHist([dominantGradients], [0], mask, self.bins, self.range)
        #         hist = cv2.normalize(hist, None)
        #         hist = np.round(hist,4)
        #         descriptor += hist
        # return descriptor

        ## parallel compute edge descriptor
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            for row in range(self.rows):
                for col in range(self.cols):
                    mask[int((frameH/self.rows)*row):int((frameH/self.rows)*(row+1)),int((frameW/self.cols)*col):int((frameW/self.cols)*(col+1))] = 255
                    descriptor_list = {executor.submit(self.hist, descriptor, dominantGradients, row, col, mask)}
        
        return descriptor


edgeHistogramComputer = EdgeHistogramComputer(12,12)
def get_edge_entropy(input_file, shot_list, return_value):
    vs = cv2.VideoCapture(input_file)
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
            descriptor = edgeHistogramComputer.compute(frame)
            total_entropy += edge_entropy(descriptor)
            break    
        frame_count += 1
        
    return_value.value = total_entropy

if __name__=="__main__":
    input_file = "/home/min/background_LiteOn_P1_2019-11-12_15:00:36.mp4"
    s = time.time()
    total_entropy = get_edge_entropy(input_file)
    print(time.time()-s)
    print(total_entropy)
