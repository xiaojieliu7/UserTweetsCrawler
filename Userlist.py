import pymongo

list = [
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
     'list': ['BBCSport', 'cnnsport', 'FOXSports', 'TMZ_Sports', 'NewsdaySports', 'SkySportsNews', 'SkySports', 'CNNFC',
              'espn', 'SportsCenter', 'NYTSports', 'YahooSports', 'SInow', 'CBSSports', 'SuperSportTV']},
    {'_id': 4,
     'type': 'Military',
     'list': ['Militarydotcom', 'MilitaryTimes', 'USArmy', 'USNavy', 'usairforce', 'ArmyRecognition', 'AirRecognition',
              'NavyRecognition', 'WeaponBlog',
              'NewsMilitaryCom', 'CanMNews', 'AllMilitaryNews', 'MilitaryAvenue', 'WarHistoryOL', 'EINMilitaryNews',
              'defense_news', 'ArmyTimes', 'ForcesNews', 'BritishArmy', 'DefenceHQ', 'WarfareMagazine',
              'USMilitaryWorld']},
    {'_id': 5,
     'type': 'Politics',
     'list': ['CNNPolitics', 'BBCPolitics', 'CBCPolitics', 'NBCPolitics', 'AP_Politics', 'GdnPolitics', 'politico',
              'thehill', 'TIMEPolitics', 'CBSPolitics', 'YahooPolitics', 'nprpolitics', 'politicshome', 'globepolitics',
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


client = pymongo.MongoClient('101.132.187.45:27017')
db = client.get_database('twitter')
collection = db.get_collection('train_user')
collection.insert(list)
