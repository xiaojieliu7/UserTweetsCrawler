import sys
import threading
from queue import Queue

import time

users_queue = Queue()
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
            if users_queue.empty():
                break
            else:
                print("Starting " + self.name)
                threadLock.acquire()
                username = users_queue.get()
                threadLock.release()
                parse(self.category, username)
                users_queue.task_done()
                print("Exiting " + self.name)


def printTweet(descr, t):
    print(descr)
    print("Username: %s" % t.username)
    print("Retweets: %d" % t.retweets)
    print("Text: %s" % t.text)
    print("Mentions: %s" % t.mentions)
    print("Hashtags: %s\n" % t.hashtags)


def parse(category, username):
    # Example 1 - Get tweets by username
    print('category:', category, '\tscreen_name:', username)
    tweetCriteria = got.manager.TweetCriteria().setUsername(username).setMaxTweets(-1)
    tweets = got.manager.TweetManager.getTweets(category=category, tweetCriteria=tweetCriteria, proxy='127.0.0.1:1080')
    # for tweet in tweets:
    #     printTweet("### Example 1 - Get tweets by username " + username, tweet)

        # Example 2 - Get tweets by query search
        # tweetCriteria = got.manager.TweetCriteria().setQuerySearch('europe refugees').setSince("2015-05-01").setUntil(
        #     "2015-09-30").setMaxTweets(1)
        # tweet = got.manager.TweetManager.getTweets(tweetCriteria)[0]
        #
        # printTweet("### Example 2 - Get tweets by query search [europe refugees]", tweet)

        # Example 3 - Get tweets by username and bound dates
        # tweetCriteria = got.manager.TweetCriteria().setUsername("barackobama").setSince("2015-09-10").setUntil(
        #     "2015-09-12").setMaxTweets(1)
        # tweet = got.manager.TweetManager.getTweets(tweetCriteria)[0]
        #
        # printTweet("### Example 3 - Get tweets by username and bound dates [barackobama, '2015-09-10', '2015-09-12']",
        #            tweet)


def initQueue(typeid):
    userlist = [
        {'_id': 1,
         'type': 'Entertainment',
         'list': ['BBCNewsEnts', 'CNNent', 'CBCEnt', 'YahooEnt', 'CBSNewsEnt', 'etnow', 'EW', 'enews', 'latimesent',
                  'APEntertainment', 'TOIEntertain', 'htshowbiz', 'CBSNewsEnt', 'MSN_Entertain', 'Marvel', 'TMZ',
                  'e_entertainment', 'THR', 'RTE_Ents', 'accesshollywood', 'NewsdayEnt', 'Variety']},
        {'_id': 2,
         'type': 'Religion',
         'list': ['CNNbelief', 'ABCReligion', 'CBSReligion', 'religionnews', 'ReligionReport', 'RNS', 'MuslimMatters',
                  'muslimvoices', 'Maestrouzy', 'islamicthought', 'islamicstrength', 'LostIslamicHist', 'HuffPostRelig',
                  'islamicfreedom']},
        {'_id': 3,
         'type': 'Sport',
         'list': ['BBCSport', 'cnnsport', 'FOXSports', 'TMZ_Sports', 'NewsdaySports', 'SkySportsNews', 'SkySports',
                  'CNNFC',
                  'espn', 'SportsCenter', 'NYTSports', 'YahooSports', 'SInow', 'CBSSports', 'SuperSportTV']},
        {'_id': 4,
         'type': 'Military',
         'list': ['Militarydotcom', 'MilitaryTimes', 'USArmy', 'USNavy', 'usairforce', 'ArmyRecognition',
                  'AirRecognition',
                  'NavyRecognition', 'WeaponBlog',
                  'NewsMilitaryCom', 'CanMNews', 'AllMilitaryNews', 'MilitaryAvenue', 'WarHistoryOL', 'EINMilitaryNews',
                  'defense_news', 'ArmyTimes', 'ForcesNews', 'BritishArmy', 'DefenceHQ', 'WarfareMagazine',
                  'USMilitaryWorld']},
        {'_id': 5,
         'type': 'Politics',
         'list': ['CNNPolitics', 'BBCPolitics', 'CBCPolitics', 'NBCPolitics', 'AP_Politics', 'GdnPolitics', 'politico',
                  'thehill', 'TIMEPolitics', 'CBSPolitics', 'YahooPolitics', 'nprpolitics', 'politicshome',
                  'globepolitics',
                  'NYDNPolitics', 'breakingpol']},
        {'_id': 6,
         'type': 'Education',
         'list': ['bbceducation', 'educationnation', 'usedgov', 'educationweek', 'educationgovuk',
                  'GuardianEdu', 'USNewsEducation', 'DiscoveryEd', 'HuffPostEdu', 'EducationNext', 'timeshighered',
                  'PostSchools']},
        {'_id': 7,
         'type': 'Technology',
         'list': ['technology', 'BBCTech', 'TechCrunch', 'fttechnews', 'CNET', 'WIRED', 'Gizmodo', 'ForbesTech']},
        {'_id': 8,
         'type': 'Economy',
         'list': ['BBCBusiness', 'TheEconomist', 'CNBC', 'EconomyWrld', 'YahooFinance', 'FinancialXpress', 'WSJ']},
        {'_id': 9,
         'type': 'Agriculture',
         'list': ['EUAgri', 'USDA', 'AgricultureTips', 'PurdueAg', 'MdAgDept', 'SKAgriculture', 'AgMuseum']}
    ]
    parselist = userlist[typeid-1]['list']
    for u in parselist:
        users_queue.put(u)
    return userlist[typeid-1]['type']

if __name__ == '__main__':
    print('start at:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    parse('Economy', 'EconomyWrld')

    # category = initQueue(int(sys.argv[1]))
    # threads = []
    # for i in range(16):
    #     # 创建新线程
    #     thread = myThread(i, "Thread-" + str(i), category)
    #     threads.append(thread)
    #     # 开启线程
    #     thread.start()
    #
    # for t in threads:
    #     t.join()

    print('end at:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
