import threading
def work(value):
    value[i]+=10
if __name__=="__main__":
    num = [10, 20]
    t1 = threading.Thread(target=work, args=(num[0],))
    t2 = threading.Thread(target=work, args=(num[1],))
    t1.start()
    t2.start()
    t2.join()
    t1.join()
    print(num)