import json
import re
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
                # 进程加锁，从队列中取用户名
                threadLock.acquire()
                screen_name = users_queue.get()
                threadLock.release()

                # 查询日志判断当前用户是否已完成，若未完成则进行tagme api调用
                collection = db.get_collection("tagme_log")
                resultlog = collection.find_one({"screen_name": screen_name})
                if resultlog is None:
                    log = {"screen_name": screen_name, "finish": False}
                    collection.insert_one(log)
                    tagtweets(screen_name)
                else:
                    if resultlog["finish"] is False:
                        tagtweets(screen_name)
                # tagme api调用完成，将用户完成标志设为True
                log = {"screen_name": screen_name, "finish": True}
                collection.update_one({"screen_name": screen_name}, {"$set": log})

                users_queue.task_done()
                print("Exiting " + self.name)


def tagtweets(screen_name):
    pattern = re.compile('https?://[^ ]*|RT @.*: |@.[^ ,]*[ ,]|&amp;|#[^ ]*', re.I)
    userinfo = db.typical.find_one({"screen_name": screen_name})
    tweets_count = len(userinfo['tweets'])
    for i in range(tweets_count):
        print(screen_name, ":\ttweet count: ", i, "/", tweets_count)
        tweet = userinfo['tweets'][i]
        language = tweet['lang']
        # 仅处理英语推文
        if language != "en":
            continue
        tweet_text = tweet['text']
        tweet_text = re.sub(pattern, r' ', tweet_text)
        # 不包含摘要和类别，类别基于DBpedia3.8，过时了
        url = 'https://tagme.d4science.org/tagme/tag?lang=en&include_abstract=false&include_categories=false' \
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
            # print(screen_name, ":\t", res.text)
            tagme_result = json.loads(res.text)
            for j in range(len(tagme_result['annotations'])):
                pageid = tagme_result['annotations'][j]["id"]
                # link_probability = tagme_result['annotations'][j]["link_probability"]
                """
                We stress here that ρ does not indicate the relevance of the entity in the input text, 
                but is rather a confidence score assigned by TagMe to that annotation. 
                You can use the ρ value to discard annotations that are below a given threshold. 
                The threshold should be chosen in the interval [0,1]. A reasonable threshold is between 0.1 and 0.3.
                """
                rho = tagme_result['annotations'][j]["rho"]
                # 如果rho小于0.1，则不处理
                if rho < 0.1:
                    continue

                categories_list = []
                # 根据pageid查找所属wiki类别
                while True:
                    try:
                        url = 'https://en.wikipedia.org/w/api.php?' \
                              'action=query&prop=categories&clshow=!hidden&cllimit=max&pageids='\
                              + str(pageid) + '&format=json'
                        res = requests.post(url=url)
                        if res.status_code == 200:
                            wikiText = json.loads(res.text)
                            if 'query' in wikiText:
                                wikiQuery = wikiText['query']
                                if 'pages' in wikiQuery:
                                    wikiPages = wikiQuery['pages']
                                    for pageid in wikiPages:
                                        page = wikiPages[pageid]
                                        if 'categories' in page:
                                            for cate in page['categories']:
                                                c = cate['title']
                                                if 'disambiguation' not in c and 'redirects' not in c:
                                                    categories_list.append(c)
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(1)
                        continue
                tagme_result['annotations'][j].setdefault("categories", categories_list)
            if "tagme" in tweet:
                tweet["tagme"] = tagme_result
            else:
                tweet.setdefault("tagme", tagme_result)
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
    for i in range(16):
        # 创建新线程
        thread = MyThread(threadID=i, name="Thread-" + str(i))
        threads.append(thread)
        # 开启线程
        thread.start()

    for t in threads:
        t.join()
    print('end at:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

