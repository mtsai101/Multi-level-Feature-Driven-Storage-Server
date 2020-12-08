from multiprocessing import Process
from virtual_camera import WorkloadGen
import os
import time
import virtual_camera
import threading
if __name__=='__main__':
    print("[INFO] running virtual camera...")
    workloadGen = WorkloadGen()



    if workloadGen.mode==1:
        try:
            t = threading.Thread(target=workloadGen.open_AP_listening_port,args=())
            t.start()
        except Exception as e:
            print(e)
        try:
            t2 = threading.Thread(target=workloadGen.open_SLE_sending_port,args=())
            t2.start()
        except Exception as e:
            print(e)

    elif workloadGen.mode==2:
        try:
            t = threading.Thread(target=workloadGen.open_DP_listening_port,args=())
            t.start()
        except Exception as e:
            print(e)
        try:
            t2 = threading.Thread(target=workloadGen.open_DDM_sending_port,args=())
            t2.start()
        except Exception as e:
            print(e)
    else:
        print("Error camera mode!")
        
    try:
        t3 = threading.Thread(target=workloadGen.check_ready,args=())
        t3.start()
        workloadGen.run()
    except Exception as e:
        print(e)


    while(True):
        time.sleep(1000)