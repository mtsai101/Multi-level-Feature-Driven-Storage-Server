
from multiprocessing import Process
import os
import time
from virtual_db_agent import DB_agent
import threading

if __name__=='__main__':
    print("[INFO] running DB agent...")
    db_agent = DB_agent()
    try:
        t = threading.Thread(target=db_agent.open_DP_listening_port,args=())
        t.start()
    except Exception as e:
        print(e)
        
    try:
        t2 = threading.Thread(target=db_agent.open_DDM_sending_port,args=())
        t2.start()
    except Exception as e:
        print(e)

    try:
        t3 = threading.Thread(target=db_agent.check_ready,args=())
        t3.start()
        db_agent.run()
    except Exception as e:
        print(e)

    while(True):
        time.sleep(1000)