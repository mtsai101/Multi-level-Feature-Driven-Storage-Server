from PIL import Image
import numpy as np
import time 

from matplotlib import pyplot as plt
import sys
import cv2
def entropy(signal):
        '''
        function returns entropy of a signal
        signal must be a 1-D numpy array
        '''
        lensig=signal.size
        symset=list(set(signal))
        numsym=len(symset)
        propab=[np.size(signal[signal==i])/(1.0*lensig) for i in symset]
        ent=np.sum([p*np.log2(1.0/p) for p in propab])
        return ent

if __name__=='__main__':
    # video entropy color histogram
    vs = cv2.VideoCapture('./storage_server_volume/raw_videos/raw_11_9/ipcam1/LiteOn_P1_2019-11-09_09:31:53.mp4')
    ret = True
    while ret:
        start_time = time.time()

        ret, frame = vs.read()

        colorIm=np.array(frame)
        S=colorIm.shape
        region=colorIm.flatten()
        print(entropy(region), 1/(time.time()-start_time))
        
    # sys.exit()


    # plt.subplot(1,3,1)
    # plt.imshow(colorIm)

    # plt.subplot(1,3,2)
    # plt.imshow(greyIm, cmap=plt.cm.gray)

    # plt.subplot(1,3,3)
    # plt.imshow(E, cmap=plt.cm.jet)
    # plt.xlabel('Entropy in 10x10 neighbourhood')
    # plt.colorbar()

    # plt.show()