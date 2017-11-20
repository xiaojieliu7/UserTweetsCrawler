import json
import threading
import time
import pymongo
import requests
from queue import Queue


class MyThread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
        while True:
            if users_queue.empty():
                break
            else:
                print("Starting " + self.name)
                threadLock.acquire()
                screen_name = users_queue.get()
                threadLock.release()

                collection = db.get_collection("tagme_log")
                resultlog = collection.find_one({"screen_name": screen_name})
                if resultlog is None:
                    log = {"screen_name": screen_name, "finish": False}
                    collection.insert_one(log)
                    tagtweets(screen_name)
                else:
                    if resultlog["finish"] is False:
                        tagtweets(screen_name)
                log = {"screen_name": screen_name, "finish": True}
                collection.update_one({"screen_name": screen_name}, {"$set": log})

                users_queue.task_done()
                print("Exiting " + self.name)


def tagtweets(screen_name):
    userinfo = db.typical.find_one({"screen_name": screen_name})
    for i in range(len(userinfo['tweets'])):
        tweet = userinfo['tweets'][i]
        tweet_text = tweet['text']
        url = 'https://tagme.d4science.org/tagme/tag?lang=en&include_abstract=true&include_categories=false' \
              '&gcube-token=492d54fa-e214-4c61-a78b-fb1760ddcbaf-843339462&' \
              'text=' + tweet_text

        while True:
            try:
                res = requests.post(url=url)
                break
            except Exception as e:
                print(e)
                time.sleep(1)
                continue
        if res.status_code == 200:
            print(screen_name, ":\t", res.text)
            if "tagme" in tweet:
                tweet["tagme"] = json.loads(res.text)
            else:
                tweet.setdefault("tagme", json.loads(res.text))
            userinfo['tweets'][i] = tweet
        else:
            continue
    db.typical.update_one({"screen_name": screen_name}, {"$set": userinfo})

if __name__ == "__main__":
    print('start at:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    users_queue = Queue()
    threadLock = threading.Lock()

    client = pymongo.MongoClient('101.132.187.45:27017')
    db = client.get_database('tweets')
    for user in db.typical.find(projection={"screen_name": True, "_id": False}):
        users_queue.put(user["screen_name"])
    threads = []
    for i in range(4):
        # 创建新线程
        thread = MyThread(threadID=i, name="Thread-" + str(i))
        threads.append(thread)
        # 开启线程
        thread.start()

    for t in threads:
        t.join()
    print('end at:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

