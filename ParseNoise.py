import sys
import threading
from queue import Queue

import time

import datetime

import gensim
import pymongo

time_queue = Queue()
threadLock = threading.Lock()

if sys.version_info[0] < 3:
    import got
else:
    import got3 as got


class myThread(threading.Thread):  # 继承父类threading.Thread
    def __init__(self, threadID, name, category):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.category = category

    def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
        while True:
            if time_queue.empty():
                break
            else:
                print("Starting " + self.name)
                threadLock.acquire()
                t = time_queue.get().split('\t')
                threadLock.release()
                parse(self.category, t[0], t[1])
                time_queue.task_done()
                print("Exiting " + self.name)


def printTweet(descr, t):
    print(descr)
    print("Username: %s" % t.username)
    print("Retweets: %d" % t.retweets)
    print("Text: %s" % t.text)
    print("Mentions: %s" % t.mentions)
    print("Hashtags: %s\n" % t.hashtags)


def parse(category, since, until):
    print('category:', category, '\t', since, '\t', until)

    querystr = queryList()
    # Example 2 - Get tweets by query search
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch(querystr).setSince(since).setUntil(until).setMaxTweets(-1)
    got.manager.TweetManager.getNoiseTweets(category=category, tweetCriteria=tweetCriteria)


def initQueue():
    now = datetime.datetime.now()
    for i in range(100):
        yesterday = now - datetime.timedelta(days=1)
        since = yesterday.strftime('%Y-%m-%d')
        until = now.strftime('%Y-%m-%d')
        # print(since, '\t', until)
        time_queue.put(since + '\t' + until)
        now = yesterday

def queryList():
    client = pymongo.MongoClient()
    db = client.get_database('Twitter')
    collection = db.get_collection('similar_words')
    categories = [c["category"] for c in collection.find(projection={"_id": False, "category": True})]
    all_wordslist = []
    for s in collection.find():
        s_similarlist = s['similar']
        similar_wordslist = []
        for w in s_similarlist:
            word = w['word']
            if word not in categories:
                similar_wordslist.append(word)
                w_similarlist = w['similar']
                similar_wordslist.extend(w_similarlist)
        templist = []
        for w in similar_wordslist:
            if w not in gensim.parsing.preprocessing.STOPWORDS:
                templist.append(w)
        all_wordslist.extend(templist)
    similar_wordset = set(all_wordslist)
    querystr = ""
    for w in similar_wordset:
        querystr = querystr + '-' + w + ' '
    return querystr


if __name__ == '__main__':
    print('start at:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    initQueue()
    threads = []
    for i in range(16):
        # 创建新线程
        thread = myThread(i, "Thread-" + str(i), category='Other')
        threads.append(thread)
        # 开启线程
        thread.start()

    for t in threads:
        t.join()

    print('end at:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
