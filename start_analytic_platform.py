import os
import threading

from analytics.analytic_main import Analytic_Platform
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
if __name__ == '__main__':
    AP = Analytic_Platform()
    try:
        t2 = threading.Thread(target=AP.open_VC_sending_port,args=())
        t2.start()

    except Exception as e:
        print(e)


    try:
        print("[INFO] running analytic platform")
        AP.run()

    except Exception as e:
        print(e)