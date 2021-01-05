import threading
import time


def sayHai(index) :
    print("hi  你好, ", index)
    time.sleep(1)

if __name__ == '__main__':
    print("main")

    # 主线程
    # for i in range(4) :
    #     sayHai()

    for i in range(4) :
        thread = threading.Thread(target=sayHai(i))
        thread.start()

    # while True:
    #     length = len(threading.enumerate())
    #     print('当前运行的线程数为：%d' % length)
    #     if length <= 1:
    #         break
    #
    #     time.sleep(0.5)