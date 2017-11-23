import gensim
import pymongo
import numpy as np
from nltk import SnowballStemmer
from nltk.corpus import stopwords


def get_similar_words():
    words_list = ['ibm', 'iphone']
    client = pymongo.MongoClient()
    db = client.get_database('Twitter')
    collection = db.get_collection('similar_words')
    for word in words_list:
        print(word)
        similar_words = model.most_similar(word, topn=50)
        similar_words_list = []
        for w in similar_words:
            if w not in words_list:
                w_similar_words = [simiword[0] for simiword in model.most_similar(w[0], topn=10)]
                tempdic = {'word': w[0], 'similar': w_similar_words}
                similar_words_list.append(tempdic)
        collection.insert_one({'category': word, 'similar': similar_words_list})
        print(similar_words)


def get_center_category(categories):
    """
    利用word2vec计算类别列表的相似度，与列表中其他类别最相似的类别作为中心类别
    :param categories: 类别列表
    :return: 中心类别，各个类别的权重列表
    """
    categoryCout = len(categories)
    similarityMatrix = np.zeros((categoryCout, categoryCout))
    for i in range(categoryCout):
        for j in range(categoryCout):
            if i == j:
                similarityMatrix[i][j] = 1
            else:
                try:
                    # 将wiki类别短语进行分词、词干化
                    categoryI = [SnowballStemmer('english').stem(w) for w in
                                 categories[i].replace("Category:", "").lower().split(' ')
                                 if w not in stopwords.words('english')]
                    categoryJ = [SnowballStemmer('english').stem(w) for w in
                                 categories[j].replace("Category:", "").lower().split(' ')
                                 if w not in stopwords.words('english')]
                    similarityMatrix[i][j] = model.n_similarity(categoryI, categoryJ)
                except KeyError as e:
                    # word not in vocabulary
                    print(e)
                    continue
    mostSimilarIndex = 0
    maxSimilarty = 0
    for i in range(categoryCout):
        sumSimilarity = sum(similarityMatrix[i])
        if sumSimilarity > maxSimilarty:
            maxSimilarty = sumSimilarity
            mostSimilarIndex = i
    weightList = []
    totalWeight = 0
    # 计算每个类别的权重
    for sim in similarityMatrix[mostSimilarIndex]:
        weight = np.math.exp(sim)
        weightList.append(weight)
        totalWeight += weight
    weightList = [w/totalWeight for w in weightList]
    return categories[mostSimilarIndex], weightList


def get_users_category_center():
    """
    读取数据库中的用户，每个用户的推文，每条推文关联的wiki实体，每个实体对应的wiki类别列表，计算每个类别列表的中心类别
    :return:
    """
    userList = []
    client = pymongo.MongoClient('101.132.187.45:27017')
    db = client.get_database('tweets')
    for user in db.typical.find(projection={"screen_name": True, "_id": False}):
        userList.append(user["screen_name"])
    for screen_name in userList:
        print("process user:\t", screen_name)
        userinfo = db.typical.find_one({"screen_name": screen_name})
        tweets_count = len(userinfo['tweets'])
        for i in range(tweets_count):
            print(screen_name, ":\t", i, "/", tweets_count)
            tweet = userinfo['tweets'][i]
            language = tweet['lang']
            # 仅处理英语推文
            if language != "en":
                continue
            if "tagme" in tweet:
                annotations = tweet["tagme"]["annotations"]
                newAnnotations = []
                for annotation in annotations:
                    # 仅处理confidence score大于0.2的结果
                    if annotation["rho"] < 0.2:
                        continue
                    # 判断类别列表是否为空
                    cList = annotation["categories"]
                    if len(cList) == 0:
                        continue
                    # 判断是否已经计算过权重
                    if len(cList) > 0 and "score" in cList[0]:
                        continue
                    # 计算中心类别，及各个类别的权重
                    centerCategory, wlist = get_center_category(annotation["categories"])
                    # print(centerCategory)
                    annotation.setdefault("top_category", centerCategory)
                    newCategories = []
                    for j in range(len(wlist)):
                        # print(annotation["categories"][j], wlist[j])
                        newCategories.append({"category": annotation["categories"][j], "score": wlist[j]})
                    annotation["categories"] = newCategories
                    newAnnotations.append(annotation)
                tweet["tagme"]["annotations"] = newAnnotations
            userinfo['tweets'][i] = tweet
        # 将结果更新到mongodb数据库
        db.typical.update_one({"screen_name": screen_name}, {"$set": userinfo})


if __name__ == "__main__":
    # GloVe Model
    model = gensim.models.KeyedVectors.load_word2vec_format('../../../gensim.glove.twitter.27B.200d.txt', binary=False)
    # model.save('model\\glove.twitter.27B.200d')
    get_users_category_center()
