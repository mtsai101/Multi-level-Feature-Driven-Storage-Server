import os
from multiprocessing import Process
from optimal_downsampling_manager.downsample_decision_maker import DownSampleDecisionMaker
import threading 


import time

if __name__ == '__main__':
    print("[INFO] Running DownSampleDecisionMaker")
    DDM = DownSampleDecisionMaker()

    try:
        t2 = threading.Thread(target=DDM.open_VC_listening_port,args=())
        t2.start()
    except Exception as e:
        print(e)
    try:
        t3 = threading.Thread(target=DDM.open_DP_sending_port,args=())
        t3.start()
    except Exception as e:
        print(e)
    try:
        t4 = threading.Thread(target=DDM.check_ready,args=())
        t4.start()
        DDM.run()
    except Exception as e:
        print(e)
    
    