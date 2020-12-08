import os
from multiprocessing import Process
from optimal_downsampling_manager.info_amount_estimator import InfoAmountEstimator
import threading 

import time

if __name__ == '__main__':
    print("[INFO] Initializing information amount estimator")
    SLE =  InfoAmountEstimator()

    try:
        t2 = threading.Thread(target=SLE.open_VC_listening_port,args=())
        t2.start()
    except Exception as e:
        print(e)

    try:
        t3 = threading.Thread(target=SLE.open_AP_sending_port,args=())
        t3.start()
    except Exception as e:
        print(e)
   
    try:
        t4 = threading.Thread(target=SLE.check_ready,args=())
        t4.start()
        SLE.run()
    except Exception as e:
        print(e)
