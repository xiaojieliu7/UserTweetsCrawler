import urllib.request, urllib.parse, urllib.error, urllib.request, urllib.error, urllib.parse, json, re, datetime, sys, \
    http.cookiejar
import time
import pymongo

from .. import models
from pyquery import PyQuery


class TweetManager:
    def __init__(self):
        pass

    @staticmethod
    def getTweets(category, tweetCriteria, receiveBuffer=None, bufferLength=100, proxy=None, mongourl='101.132.187.45:27017'):
        refreshCursor = ''
        client = pymongo.MongoClient(mongourl)
        db = client.get_database('tweet')
        collection = db.get_collection('log')
        resultlog = collection.find_one({'screen_name': tweetCriteria.username})
        if resultlog is not None:
            refreshCursor = resultlog['refreshCursor']

        results = []
        resultsAux = []
        cookieJar = http.cookiejar.CookieJar()

        active = True

        tweets_count = 0
        while active:
            json = TweetManager.getJsonReponse(tweetCriteria, refreshCursor, cookieJar, proxy)
            if len(json['items_html'].strip()) == 0:
                break

            refreshCursor = json['min_position']
            tweets = PyQuery(json['items_html'])('div.js-stream-tweet')

            if len(tweets) == 0:
                break

            for tweetHTML in tweets:
                tweetPQ = PyQuery(tweetHTML)
                tweet = models.Tweet()

                usernameTweet = tweetPQ("span.username.js-action-profile-name b").text()
                txt = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text().replace('# ', '#').replace('@ ', '@'))
                retweets = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr(
                    "data-tweet-stat-count").replace(",", ""))
                favorites = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr(
                    "data-tweet-stat-count").replace(",", ""))
                dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"));
                id = tweetPQ.attr("data-tweet-id")
                permalink = tweetPQ.attr("data-permalink-path")
                user_id = int(tweetPQ("a.js-user-profile-link").attr("data-user-id"))

                geo = ''
                geoSpan = tweetPQ('span.Tweet-geo')
                if len(geoSpan) > 0:
                    geo = geoSpan.attr('title')
                urls = []
                for link in tweetPQ("a"):
                    try:
                        urls.append((link.attrib["data-expanded-url"]))
                    except KeyError:
                        pass
                tweet.id = id
                tweet.permalink = 'https://twitter.com' + permalink
                tweet.username = usernameTweet

                tweet.text = txt
                tweet.date = datetime.datetime.fromtimestamp(dateSec)
                tweet.formatted_date = datetime.datetime.fromtimestamp(dateSec).strftime("%a %b %d %X +0000 %Y")
                tweet.retweets = retweets
                tweet.favorites = favorites
                tweet.mentions = " ".join(re.compile('(@\\w*)').findall(tweet.text))
                tweet.hashtags = " ".join(re.compile('(#\\w*)').findall(tweet.text))
                tweet.geo = geo
                tweet.urls = ",".join(urls)
                tweet.author_id = user_id

                results.append(tweet)
                resultsAux.append(tweet)

                tweets_count += 1
                if tweets_count % 100 == 0:
                    temp_results = results
                    insertcount = 0
                    for t in temp_results:
                        tweet = {'_id': t.id,
                                    'screen_name': tweetCriteria.username,
                                    'date': t.date.strftime("%Y-%m-%d %H:%M"),
                                    'retweets': t.retweets,
                                    'favorites': t.favorites,
                                    'text': t.text,
                                    'geo': t.geo,
                                    'mentions': t.mentions,
                                    'hashtags': t.hashtags}
                        try:
                            collection.insert(tweet)
                            insertcount += 1
                        except Exception as e:
                            print(e)
                            continue
                    collection = db.get_collection('log')
                    resultlog = collection.find_one({'screen_name': tweetCriteria.username})
                    if resultlog is None:
                        log = {'category': category,
                               'screen_name': tweetCriteria.username,
                               'count': insertcount,
                               'finish': False,
                               'refreshCursor': refreshCursor}
                        collection.insert(log)
                    else:
                        resultlog['count'] = resultlog['count'] + insertcount
                        resultlog['refreshCursor'] = refreshCursor
                        collection.update_one({'screen_name': tweetCriteria.username}, {"$set": resultlog})
                    print("user_name: " + tweetCriteria.username + "\tcount: " + str(tweets_count))
                    results = []

                if receiveBuffer and len(resultsAux) >= bufferLength:
                    receiveBuffer(resultsAux)
                    resultsAux = []

                if 0 < tweetCriteria.maxTweets <= tweets_count:
                    active = False
                    break

        if receiveBuffer and len(resultsAux) > 0:
            receiveBuffer(resultsAux)

        temp_results = results
        insertcount = 0
        for t in temp_results:
            tweet = {'_id': t.id,
                     'screen_name': tweetCriteria.username,
                     'date': t.date.strftime("%Y-%m-%d %H:%M"),
                     'retweets': t.retweets,
                     'favorites': t.favorites,
                     'text': t.text,
                     'geo': t.geo,
                     'mentions': t.mentions,
                     'hashtags': t.hashtags}
            try:
                collection.insert(tweet)
                insertcount += 1
            except Exception as e:
                print(e)
                continue

        collection = db.get_collection('log')
        resultlog = collection.find_one({'screen_name': tweetCriteria.username})
        if resultlog is None:
            log = {'category': category,
                   'screen_name': tweetCriteria.username,
                   'count': 0,
                   'finish': True,
                   'refreshCursor': refreshCursor}
            collection.insert(log)
        else:
            resultlog['finish'] = True
            collection.update_one({'screen_name': tweetCriteria.username}, {"$set": resultlog})
        return results

    @staticmethod
    def getNoiseTweets(category, tweetCriteria, receiveBuffer=None, bufferLength=100, proxy=None, mongourl='101.132.187.45:27017'):
        refreshCursor = ''
        client = pymongo.MongoClient(mongourl)
        db = client.get_database('tweet')
        collection = db.get_collection('noise_log')
        resultlog = collection.find_one({'until': tweetCriteria.until})
        if resultlog is not None:
            refreshCursor = resultlog['refreshCursor']

        results = []
        resultsAux = []
        cookieJar = http.cookiejar.CookieJar()

        active = True

        tweets_count = 0
        while active:
            json = TweetManager.getJsonReponse(tweetCriteria, refreshCursor, cookieJar, proxy)
            if len(json['items_html'].strip()) == 0:
                break

            refreshCursor = json['min_position']
            tweets = PyQuery(json['items_html'])('div.js-stream-tweet')

            if len(tweets) == 0:
                break

            for tweetHTML in tweets:
                tweetPQ = PyQuery(tweetHTML)
                tweet = models.Tweet()

                usernameTweet = tweetPQ("span.username.js-action-profile-name b").text()
                txt = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text().replace('# ', '#').replace('@ ', '@'))
                retweets = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr(
                    "data-tweet-stat-count").replace(",", ""))
                favorites = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr(
                    "data-tweet-stat-count").replace(",", ""))
                dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"));
                id = tweetPQ.attr("data-tweet-id")
                permalink = tweetPQ.attr("data-permalink-path")
                user_id = int(tweetPQ("a.js-user-profile-link").attr("data-user-id"))

                geo = ''
                geoSpan = tweetPQ('span.Tweet-geo')
                if len(geoSpan) > 0:
                    geo = geoSpan.attr('title')
                urls = []
                for link in tweetPQ("a"):
                    try:
                        urls.append((link.attrib["data-expanded-url"]))
                    except KeyError:
                        pass
                tweet.id = id
                tweet.permalink = 'https://twitter.com' + permalink
                tweet.username = usernameTweet

                tweet.text = txt
                tweet.date = datetime.datetime.fromtimestamp(dateSec)
                tweet.formatted_date = datetime.datetime.fromtimestamp(dateSec).strftime("%a %b %d %X +0000 %Y")
                tweet.retweets = retweets
                tweet.favorites = favorites
                tweet.mentions = " ".join(re.compile('(@\\w*)').findall(tweet.text))
                tweet.hashtags = " ".join(re.compile('(#\\w*)').findall(tweet.text))
                tweet.geo = geo
                tweet.urls = ",".join(urls)
                tweet.author_id = user_id

                results.append(tweet)
                resultsAux.append(tweet)

                tweets_count += 1
                if tweets_count % 100 == 0:
                    temp_results = results
                    insertcount = 0
                    for t in temp_results:
                        tweet = {'_id': t.id,
                                    'screen_name': tweetCriteria.username,
                                    'date': t.date.strftime("%Y-%m-%d %H:%M"),
                                    'retweets': t.retweets,
                                    'favorites': t.favorites,
                                    'text': t.text,
                                    'geo': t.geo,
                                    'mentions': t.mentions,
                                    'hashtags': t.hashtags}
                        try:
                            collection.insert(tweet)
                            insertcount += 1
                        except Exception as e:
                            print(e)
                            continue
                    collection = db.get_collection('noise_log')
                    resultlog = collection.find_one({'until': tweetCriteria.until})
                    if resultlog is None:
                        log = {'category': category,
                               'until': tweetCriteria.until,
                               'count': insertcount,
                               'finish': False,
                               'refreshCursor': refreshCursor}
                        collection.insert(log)
                    else:
                        resultlog['count'] = resultlog['count'] + insertcount
                        resultlog['refreshCursor'] = refreshCursor
                        collection.update_one({'until': tweetCriteria.until}, {"$set": resultlog})
                    print("until: " + tweetCriteria.until + "\tcount: " + str(tweets_count))
                    results = []

                if receiveBuffer and len(resultsAux) >= bufferLength:
                    receiveBuffer(resultsAux)
                    resultsAux = []

                if 0 < tweetCriteria.maxTweets <= tweets_count:
                    active = False
                    break

        if receiveBuffer and len(resultsAux) > 0:
            receiveBuffer(resultsAux)

        temp_results = results
        insertcount = 0
        for t in temp_results:
            tweet = {'_id': t.id,
                     'screen_name': tweetCriteria.username,
                     'date': t.date.strftime("%Y-%m-%d %H:%M"),
                     'retweets': t.retweets,
                     'favorites': t.favorites,
                     'text': t.text,
                     'geo': t.geo,
                     'mentions': t.mentions,
                     'hashtags': t.hashtags}
            try:
                collection.insert(tweet)
                insertcount += 1
            except Exception as e:
                print(e)
                continue

        collection = db.get_collection('noise_log')
        resultlog = collection.find_one({'until': tweetCriteria.until})
        if resultlog is None:
            log = {'category': category,
                   'until': tweetCriteria.until,
                   'count': 0,
                   'finish': True,
                   'refreshCursor': refreshCursor}
            collection.insert(log)
        else:
            resultlog['finish'] = True
            collection.update_one({'until': tweetCriteria.until}, {"$set": resultlog})
        return results

    @staticmethod
    def getJsonReponse(tweetCriteria, refreshCursor, cookieJar, proxy):
        url = "https://twitter.com/i/search/timeline?f=tweets&q=%s&src=typd&%smax_position=%s"

        urlGetData = ''
        if hasattr(tweetCriteria, 'username'):
            urlGetData += ' from:' + tweetCriteria.username

        if hasattr(tweetCriteria, 'since'):
            urlGetData += ' since:' + tweetCriteria.since

        if hasattr(tweetCriteria, 'until'):
            urlGetData += ' until:' + tweetCriteria.until

        if hasattr(tweetCriteria, 'querySearch'):
            urlGetData += ' ' + tweetCriteria.querySearch

        if hasattr(tweetCriteria, 'lang'):
            urlLang = 'lang=' + tweetCriteria.lang + '&'
        else:
            urlLang = ''
        url = url % (urllib.parse.quote(urlGetData), urlLang, refreshCursor)
        # print(url)

        headers = [
            ('Host', "twitter.com"),
            ('User-Agent', "Mozilla/5.0 (Windows NT 6.1; Win64; x64)"),
            ('Accept', "application/json, text/javascript, */*; q=0.01"),
            ('Accept-Language', "de,en-US;q=0.7,en;q=0.3"),
            ('X-Requested-With', "XMLHttpRequest"),
            ('Referer', url),
            ('Connection', "keep-alive")
        ]

        if proxy:
            # urllib.request.urlopen("https://www.baidu.com")
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({'http': proxy, 'https': proxy}),
                                                 urllib.request.HTTPCookieProcessor(cookieJar))
        else:
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookieJar))
        opener.addheaders = headers
        while True:
            try:
                response = opener.open(url)
                jsonResponse = response.read()
                break
            except Exception as e:
                print(e)
                # print("Twitter weird response. Try to see on browser: ", url)
                print(
                    "Twitter weird response. Try to see on browser: https://twitter.com/search?q=%s&src=typd" % urllib.parse.quote(
                        urlGetData))
                print("Unexpected error:", sys.exc_info()[0])
                time.sleep(1)
                continue

        dataJson = json.loads(jsonResponse.decode())

        return dataJson


def printTweet(descr, t):
    print(descr)
    print("Username: %s" % t.username)
    print("Retweets: %d" % t.retweets)
    print("Text: %s" % t.text)
    print("Mentions: %s" % t.mentions)
    print("Hashtags: %s\n" % t.hashtags)
