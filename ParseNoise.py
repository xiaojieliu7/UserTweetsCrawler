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

    querystr = "-electrical -crossing -race -sport -financial -swimmers -industrial -strategies -consider -shows " \
               "-libertarians -ukrainian -multiplayer -service -orioles -allah -association -arsenal -sports " \
               "-construction -analytics -q&a -cricket -afghan -real -english -bball -medal -mortgage -architecture " \
               "-hosted -damascus -labor -claims -journalist -meetings -musician -topic -fitness -facilities -guide " \
               "-murdered -family -fda -athiest -feminist -islamic -primary -progressives -deployments -teams " \
               "-nominated -inequity -resource -sustainable -ahead -peek -gov -sessions -bahrain -handball -gdp " \
               "-programme -all-star -conservation -university -diversity -palestinians -china -promotion -kardashian " \
               "-quran -players -nyc -marxism -comments -campaigns -security -violent -guns -news -voters -hosting " \
               "-entire -playoff -exist -n't -disagree -golf -output -christ -married -catholicism -opening -ravens " \
               "-tigers -endangered -bts -connections -christmas -casualties -electricity -time -habitat -culture " \
               "-process -epl -nationalities -society -pitch -farm -lesson -united -mnet -ecosystem -host -fears " \
               "-gears -o-line -prices -cir -games -funny -science -sequestration -business -nfl -freshman -dodgers " \
               "-won -athlete -agchat -candidates -americans -governance -xmas -sment -starship -intelligence " \
               "-actress -democracy -going -eagles -performing -govt -managers -listening -jobless -badminton " \
               "-yankees -agama -fuels -commerce -nasdaq -skill -australian -hilarious -humanism -muslim -yemen " \
               "-studio -biochemistry -museum -xbox -areas -cyprus -journal -inspections -global -preschool -journo " \
               "-tv -airforce -crm -channels -deployment -keyboard -agents -ent -civilians -policemen -present " \
               "-celebrity -studios -firearms -reading -almighty -regardless -marxist -inequality -medals -ministers " \
               "-product -deficit -daily -linkedin -japanese -club -imperialism -wounded -taliban -mitt -reuters " \
               "-oversea -finance -fiscal -meditation -broadcast -seo -infotainment -european -honest -soldier -hamas " \
               "-fund -sky -session -wwe -cristo -wrestlers -sonnenzentrum -important -beings -marriage -justice " \
               "-liberals -doomed -dictatorship -worldwide -latest -mid-market -pressures -academy -unmanned " \
               "-engineers -week -iraqi -phone -exhibition -authorities -social -spiritual -improvement -privacy " \
               "-institution -venue -christians -members -gmo -wage -hope -elections -politician -presence -tourist " \
               "-artists -crossfit -jihad -capitalism -humans -laptop -studies -customer -faculty -rifle -mining " \
               "-features -headlines -mobile -wimbledon -updated -wwii -gadgets -caste -electronics -useful " \
               "-communication -posts -nationalism -truth -god -strategy -information -solutions -overseas " \
               "-volunteers -gov. -tank -agnostic -new -medicare -info -iranian -weapon -probably -rappers -nhl " \
               "-regulatory -symposium -edition -absence -holidays -point -protest -designs -progress -knicks " \
               "-agribusiness -kinda -consultant -commentary -prod -communism -independence -practice -marketing -irs " \
               "-candidate -lord -high -france -reports -wanna -affairs -assessment -drought -vogue -aid -stocks " \
               "-children -benefits -schools -agriculture -broadcasts -president -launch -democrat -entrepreneurs " \
               "-success -landscape -matter -cybersecurity -unemployment -camera -mike -ios -manufacturing -imf -’s " \
               "-ecosystems -rookie -tune -programs -missile -growers -charges -attacks -private -azealia -blame " \
               "-naval -biology -graduate -entertaining -continue -battlefield -animal -pension -austerity -exec " \
               "-investors -legislation -engineering -co-host -affordable -atheist -international -soviet -adults " \
               "-korean -churches -foodwaste -wait -discussed -wildlife -kbs -materials -deployed -wooyoung -trust " \
               "-regime -foodsecurity -recorded -wrestler -airline -ukip -amazing -sequester -plus -abilities -vets " \
               "-arrest -architects -life -ministry -deploy -exam -defense -enlightenment -jews -employees -release " \
               "-constitutional -ukraine -byod -mixtape -theist -cinema -london -yemeni -care -answer -book -partisan " \
               "-arts -bodybuilding -stars -ptsd -investigation -hospitality -events -decline -feminism -contrary " \
               "-blogging -upcoming -econ -communications -benefit -delhi -ecological -prime -street -insurance " \
               "-homework -trainee -singer -revelation -national -athletics -premier -paralympic -canada -senate " \
               "-funds -guard -bollywood -airbus -innovations -wrong -paying -overhaul -area -capitalist -communities " \
               "-wise -soccer -govts -carrier -obama -teaching -waterways -federer -indonesia -disabled -near -apps " \
               "-economics -politics -ops -answers -militants -ethics -masculinity -deploying -theatre -concert " \
               "-mayweather -grammys -equity -psychology -zionist -publichealth -insights -gender -banks -usda " \
               "-paralympics -babies -computers -issue -white -senator -learning -wwi -ammunition -poverty -bless " \
               "-swimmer -hardware -presentation -theater -terrorists -abuse -weird -conference -socialcare -sector " \
               "-lessons -economic -accessories -ethnicities -institutions -patience -industry -fees -crime -gifts " \
               "-despite -morality -smartphone -producer -employment -interview -africa -innovative -families " \
               "-nominations -believing -germany -climate -nz -dignity -wages -jesus -seminar -jobs " \
               "-アースミュージックアンドエコロジー -playoffs -highlights -flavor -medical -human -training -barcelona -assistant " \
               "-nationalist -watch -empowerment -leaders -forces -football -technician -oil -policies -cut " \
               "-donations -aircraft -stand-up -relationship -wetlands -dems -simulator -uk -reform -art -market " \
               "-showcase -pakistan -loans -canadian -medicaid -singers -helicopter -play -thoughts -campaign " \
               "-obesity -prayer -nutrition -fbi -bowling -soldiers -downturn -centers -televisión -learn -somalia " \
               "-highschool -gymnasts -kobe -shares -budget -church -awareness -youth -theology -interests -margin " \
               "-watched -thank -hagel -talks -brit -leftist -hill -boyz -r&d -economies -comedy -nationals " \
               "-interesting -warming -college -telecoms -campuses -procurement -israel -environmental -journalists " \
               "-saturday -government -airing -europe -crimes -ecb -annual -angeles -consulting -artist -songs -mlb " \
               "-public -tonight -ask -blue -gq -amen -alleged -article -explain -softball -sects -rugby -classic " \
               "-officers -analyst -poaching -buddhism -retired -t.v -interactive -currencies -u.s -organic -sectors " \
               "-beliefs -organisation -match -history -morals -clinton -healthcare -britain -guards -smh -christian " \
               "-charged -horse -election -healthy -retail -inflation -tradition -djs -enterprise -officer -cardio " \
               "-eurozone -room -bigdata -extremist -ethnicity -pop -awesome -expertise -lakers -details -experience " \
               "-u.n. -states -surplus -syria -geography -cheer -jype -secretary -corruption -syrian -border -design " \
               "-marriages -breaking -stereotypical -special -physics -fascinating -groups -biotech -plays -exchange " \
               "-foxsports -honoring -property -exclusive -rap -abroad -police -cowboys -spirituality -update " \
               "-portfolio -losses -team -lnp -power -local -amid -weapons -bible -boxing -sikhs -discussions " \
               "-coalition -northern -traders -journalism -mnc -year-old -skills -communal -nazi -initiative " \
               "-official -olympics -pakistani -award -reason -story -today -cloud -administration -holocaust " \
               "-customers -pastor -services -competitiveness -import -projects -black -obamacare -actions -battle " \
               "-organizations -wisdom -libya -tree -youths -spirit -game -conversation -aquaculture -celebration " \
               "-seal -fear -espn -ceremony -ghosts -biotechnology -continued -savior -institute -county -improv " \
               "-knowledge -math -freedom -listen -transportation -troubles -job -euro-zone -nationality -desktop " \
               "-dept. -swedish -forests -sox -networks -department -federal -chelsea -awar -rape -tcot -central " \
               "-york -rhetoric -heritage -racial -nat'l -consolidation -interviewed -sharia -attack -violence " \
               "-playin -child -reforms -defra -celebrities -accounting -strategic -students -fox -assault -sexuality " \
               "-bloomberg -assistance -democratic -coach -catholic -officials -tools -equities -cell -woollim " \
               "-learned -functionality -animals -wrestling -basketball -asking -stereotypes -lecture -u.k -taught " \
               "-renewables -fam -elementary -technologies -startups -integration -coaches -celebs -companies -scheme " \
               "-cbs -army -workers -same-sex -fisheries -workforce -streaming -fighter -fishery -digital -liberty " \
               "-teacher -enforce -league -donation -plane -looks -growth -magazines -songwriter -baseball -israeli " \
               "-branding -t.v. -biodiversity -shop -countries -patriots -relationships -awards -afghanistan -tactics " \
               "-readiness -project -presidential -bbc -liberal -world -opinions -democrats -export -businesses -ufc " \
               "-military -middle -sailors -including -brands -british -stock -pledis -champ -jyp -exports -editing " \
               "-moscow -selection -literature -championships -filming -regional -boeing -regulations -farmer -mtv " \
               "-reduced -memorial -ist -universal -robbery -read -umat -marlins -votes -extremists -strength -clubs " \
               "-check -systems -beat -tory -war -vehicles -distribution -sniper -economy -imports -offense " \
               "-academics -city -veterans -radical -advocacy -innovation -garden -bands -mankind -festival -decision " \
               "-taiwan -loan -parliament -drones -republicans -played -mfg -panetta -coverage -understanding -web " \
               "-homes -discussion -fraud -arrested -sciences -mma -different -amend -solar -retailers -tele " \
               "-religion -german -evangelical -community -compliance -tunes -ucl -questions -holy -gift -hall " \
               "-designers -ideals -deficits -influence -safety -metrics -thanksgiving -practices -fuel -existence " \
               "-question -firm -wins -combat -efficiency -warfare -yahoo -buddhist -netball -coaching -jewish " \
               "-production -audio -base -firefighters -washington -users -borders -corporation -education " \
               "-anthropology -cities -tournament -disney -organization -draft -valentine -forestry -speech -biomass " \
               "-funded -says -usmc -leadership -rural -korea -socialist -day -exercise -activities -recession " \
               "-non-profit -gymnastics -presents -deflation -plant -socialism -vball -rebels -christianity " \
               "-immigrants -investments -relief -conservatives -scientific -regulators -debut -music -afghans -polls " \
               "-futures -insurgents -ads -allegations -agree -banking -amnesty -farms -ecology -tech -title " \
               "-workshop -assad -foreign -yard -hhs -place -travel -peace -dhs -parties -laws -updates -shame " \
               "-fantastic -forum -trading -patrol -faiths -integrity -ireland -scriptures -venues -carpet -farmers " \
               "-compassion -screen -murder -maths -entertaiment -initiatives -traditions -volleyball -microbiology " \
               "-event -force -worship -revenue -firms -boi -youtube -champs -duty -grants -catholics -scripture " \
               "-cops -talented -muslims -disparity -entertainment -debates -development -labour -superstar -boarder " \
               "-'s -barack -bjp -cuts -repubs -engineer -socent -student -ipads -talent -recording -fascism -data " \
               "-celtics -hollywood -drone -tool -prosecutors -workshops -⚾ -teammate -woes -singapore -russian " \
               "-civil -lebron -emmy -standards -smartphones -millennials -emerging -bailout -islamist -cheerleaders " \
               "-olympians -architect -kids -cosmopolitan -professors -disparities -lawmakers -discrimination " \
               "-surveillance -equality -understand -musicians -infrastructure -park -solider -computing -website " \
               "-illiteracy -brand -comms -pay -surprise -featured -lib -towns -let -america -stationed -welfare " \
               "-winner -networking -navy -doj -famous -telecommunications -advertising -non-muslims -african -spying " \
               "-inequalities -comedian -minister -stage -phillies -comic -localgov -aleppo -nuclear -asia -defensive " \
               "-lives -immigration -irrelevant -accountability -forecasts -movie -matters -broadway -reporting -ammo " \
               "-abortion -transparency -broadcasting -tourism -tennis -internship -sociology -subsidies " \
               "-advertisement -suspect -globalhealth -celeb -health -library -philosophy -irrespective -pakistanis " \
               "-orgs -group -investigative -academic -animation -television -testament -existance -grace -foodsafety " \
               "-arab -romney -republican -residents -acknowledge -enforcement -political -entrepreneurship -solution " \
               "-internet -ignorant -clients -farming -channel -ministries -winning -electronic -technology -member " \
               "-app -products -ground -belief -ipods -religious -teachers -change -colleges -work -declaration " \
               "-great -value -purpose -people -climatechange -meeting -spain -sportscenter -report -recent -gym " \
               "-virtualization -agricultural -gathering -know -steelers -party -tourney -lack -female -learners " \
               "-educational -housing -years -egypt -productions -miliband -constitution -guests -field -scholarship " \
               "-coming -verses -fields -biden -terrorist -lacrosse -agencies -gop -classes -matt -dont -photoshoot " \
               "-politicians -quality -asset -cast -disgrace -organisations -australia -hijab -tax -watching -markets " \
               "-islam -policy -paid -principles -ethnic -year -secularism -discussing -nikon -qb -album -stewardship " \
               "-monsanto -rights -creative -footy -fan -defence -pacquiao -determination -access -corporations -mps " \
               "-buddhists -albums -idf -company -possible -grammy -center -atheists -sustainability -consumer " \
               "-consciousness -architectural -photography -personal -nbc -impressive -methods -study -player " \
               "-athletes -producers -continuing -efficient -productivity -congress -law -governments -pastors " \
               "-rifles -strange -wars -nba -wealth -fnc -ideological -regions -civilian -bank -providers -costs " \
               "-narrative -billboard -socialmedia -pentagon -governor -variety -cottage -fsa -partnership -program " \
               "-laptops -offensive -software -autism -emergency -implementation -hosts -teach -veteran -discuss " \
               "-teaches -cop -magazine -newspaper -woolim -marines -marine -sochi -corporate -devices -cultures " \
               "-machinery -ideology -amendment -corrupt -biblical -manager -techniques -talking -survey -iraq " \
               "-preference -olympic -fight -division -bureau -mechanical -globaldev -champions -pharma -russia " \
               "-rates -produced -u.s. -homelessness -opinion -numeracy -tories -expo -consumers -england -potus " \
               "-gods -kickboxing -bar -renewable -maritime -armed -feature -investment -debate -secular -class " \
               "-writers -relations -cod -e-commerce -bankers -cost -commodities -tycoons -country -network " \
               "-wonderful -gonna -divorce -pc -literacy -staff -effectiveness -taxes -divisive -good -developer " \
               "-custom -humanity -judaism -andme -cultural -warns -approach -funding -pilates -libs -automation " \
               "-employers -dept -homs -sales -palestinian -rapper -rehearsal -score -featuring -atheism -citizens " \
               "-clothing -revenues -hockey -star -ipod -rebel -debt -citizenship -energy -values -graphic -guest " \
               "-gameplay -environment -device -playing -protection -phones -wellness -concerns -secondary -hindus " \
               "-interface -workout -religions -death -crony -deputy -hillary -championship -martial -boss " \
               "-management -decency -slowdown -actually -school -teammates -resources -centre -praise -aviation " \
               "-iran -template -tomorrow -mariners -budgets -ibm -meaning -pollution -ideologies -writer -terrorism " \
               "-interviews -troops -appliances -developing -talk -increase -office -conservative -run -faith " \
               "-automotive -state -client -hypocrisy -crew -campus -debts -hindu -manmohan -buildings -japan " \
               "-designer -lebanon -song -oman -foreigners -investing -leader -cutting -cheerleader -gobel -radio " \
               "-shinee -italian -exciting -capital -american -olympian -ecommerce -research -universities -think " \
               "-startup "
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
