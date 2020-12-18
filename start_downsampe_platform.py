import os
import threading
from multiprocessing import Process

from downsampling.transformer_main import DownSample_Platform

if __name__ == '__main__':
    DP = DownSample_Platform()
    try:
        t2 = threading.Thread(target=DP.open_DBA_sending_port,args=())
        t2.start()

    except Exception as e:
        print(e)
    
    try:
        print("[INFO] running downsample platform")
    
        DP.run()

    except Exception as e:
        print(e)