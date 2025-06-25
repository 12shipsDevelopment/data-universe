import redis
import simplejson as json
from datetime import datetime, timedelta
from common.data import TimeBucket, DataSource

TASK_QUEUE_KEY="x:label:task_queue" # list
TASK_ADDED_KEY="x:label:task_added"  # set
TASK_COMPLETED_KEY="x:label:task_completed" # set timeBucketId

DEFAULT_LABELS = [
    "#riyadh",
    "#pr",
    "#tiktok",
    "#ad",
    "#jeddah",
    "#сryрtо",
    "#ピッコマ",
    "#yahooニュース",
    "#xrp",
    "#whalestorexoxo",
    "#انقاذ_النصر_يالصندوق",
    "#iran",
    "#sb19",
    "#enhypen",
    "#goスト",
    "#aiforall",
    "#lorealparisworthitthxmilklove",
    "#loveislandusa",
    "#standagainstninestar",
    "#ชาบูชิชวนกรี๊ดskynani",
    "#sixtonesann",
    "#btc",
    "#阪神タイガース",
    "#bitcoin",
    "#seventeen",
    "#wearemadleen",
    "#gold",
    "#lovefighters",
    "#เน็ตฟลิกซ์ราคาถูก",
    "#terstegenout",
    "#granhermano",
    "#gquuuuuux",
    "#zonauang",
    "#israel",
    "#แจกจริง",
    "#นมโอ๊ตแบรนด์เดียวที่หลิงluv",
    "#mygoldenbloodfinalep",
    "#عاجل",
    "#lacasadelosfamososcol",
    "#drallygrandopeningxperthsantaxdomiia",
    "#khobar",
    "#おはようvtuber",
    "#ดูดวง",
    "#sfxpondphuwinpermpoon",
    "#भगवान_के_चश्मदीद_गवाह",
    "#تدخل_الصندوق_مطلب_نصراوي",
    "#emdistrictxwilliamest",
    "#dammam",
    "#परमात्मा_न_जन्मताहै_न_मरताहै",
    "#fogochain",
    "#अनोखी_प्रदर्शनी",
    "#thenextprinceep7",
    "#sbhawks",
    "#kalyugmein_satyugki_shuruaat",
    "#تدخل_الصندوق_مطلب_نصراوي5",
    "#महापापी_जो_करे_जीवहिंसा",
    "#नशा_नाशका_कारणहै",
    "#วอลเลย์บอลหญิง",
    "#williamestfanconpresstour",
    "#secretstory",
    "#薬屋のひとりごと",
    "#iriam",
    "#lazada66ลดจริงกว่าใคร",
    "#दहेज_दानव_से_मुक्ति",
    "#whalestorexoxospecial",
    "#निमंत्रणसंसारको_सम्मानकेसाथ",
    "#trump",
    "#परमात्मा_की_पहचान",
    "#परमार्थ_कीअनोखी_मिसाल",
    "#अध्यात्म_का_बेड़ा_गर्क_करदिया",
    "#ジークアクス",
    "#jhope",
    "#الهلال_ريال_مدريد",
    "#nosaint_everate_meat",
    "#ラヴィット",
    "#انقذو_النصر_من_ادارته",
    "#whois_kaal_inbhagavadgita",
    "#crypto",
    "#إيران",
    "#theexmorningep5",
    "#amazon",
    "#sparklexlinglingevent",
    "#يوم_عرفة",
    "#جانم_فدای_ایران",
    "#baystars",
    "#riyadhseason",
    "#マシュマロを投げ合おう",
    "#نحن_مع_غزة",
    "#chibalotte",
    "#planecrash",
    "#freedomconvoy",
    "#طهران",
    "#thenextprinceep4",
    "#コレコレ",
    "#ormxbvlgarichidlom",
    "#ormxomegagalanight",
    "#thenextprinceep6",
    "#2025btsfesta",
    "#モンスト",
    "r/askreddit",
    "r/aitah",
    "r/amioverreacting",
    "r/politics",
    "r/nostupidquestions",
    "r/amitheasshole",
    "r/nba",
    "r/worldnews",
    "r/jerkoffchat",
    "r/wallstreetbets",
    "r/teenagers",
    "r/gooned",
    "r/relationship_advice",
    "r/soccer",
    "r/advice",
    "r/interestingasfuck",
    "r/mildlyinfuriating",
    "r/loveislandusa",
    "r/dirtyr4r",
    "r/monopoly_go",
    "r/helldivers",
    "r/kinktown",
    "r/redditgames",
    "r/changemyview",
    "r/nightreign",
    "r/rusaskreddit",
    "r/marvelrivals",
    "r/squaredcircle",
    "r/hockey",
    "r/movies",
    "r/chatgpt",
    "r/askmenadvice",
    "r/deltarune",
    "r/christianity",
    "r/pokemongoraids",
    "r/greece",
    "r/neoliberal",
    "r/teenagersbutbetter",
    "r/unpopularopinion",
    "r/askuk",
    "r/cricket",
    "r/onlyfans101brandnew",
    "r/alexandriaegy",
    "r/teenindia",
    "r/pcmasterrace",
    "r/formula1",
    "r/publicfreakout",
    "r/fauxmoi",
    "r/gaming",
    "r/facepalm",
    "r/millennials",
    "r/tennis",
    "r/pics",
    "r/topcharactertropes",
    "r/hentaiandroleplayy",
    "r/starwars",
    "r/askredditafterdark",
    "r/expedition33",
    "r/cats",
    "r/indiasocial",
    "r/news",
    "r/europe",
    "r/genx",
    "r/askmen",
    "r/nederlands",
    "r/chaoticgood",
    "r/powerscaling",
    "r/nintendoswitch2",
    "r/animequestions",
    "r/canada",
    "r/sipstea",
    "r/gunaccessoriesforsale",
    "r/deadbydaylight",
    "r/genshin_impact",
    "r/productivitycafe",
    "r/nbatalk",
    "r/50501",
    "r/baseball",
    "r/jeeneetards",
    "r/unitedkingdom",
    "r/nextfuckinglevel",
    "r/promoteonlyfans",
    "r/golf",
    "r/popculturechat",
    "r/ufc",
    "r/damnthatsinteresting",
    "r/technology",
    "r/videogames",
    "r/genshin_impact_leaks",
    "r/mapporn",
    "r/riddonkulous",
    "r/indianteenagers",
    "r/mildlyinteresting",
    "r/originalcharacter",
    "r/darkdungeongame",
    "r/whatisit",
    "r/todayilearned",
    "r/2007scape",
    "r/anime",
    "r/vent"
]

class LabelScheduler:
    @classmethod
    def from_conn(cls, host: str, port: int, password: str):
        r = redis.Redis(host=host, port=port, db=0, password=password)
        return cls(r)

    def __init__(self, r, labels :list[str] = DEFAULT_LABELS):
        self.r = r
        self.labels = labels
        self.total = []
        self.trends = []
        self.labels.reverse()

    def __key(self, label, bucketId):    
        return f"{bucketId}-{label}"

    def get_task(self):
        lua = """
        local task_data = redis.call('RPOP', KEYS[1])
        if not task_data then
            return nil
        end

        local task = cjson.decode(task_data)

        -- 使用 bucketId-label 格式生成 key
        local set_key = task.timeBucketId .. "-" .. task.label
        redis.call('SREM', KEYS[2], set_key)

        return task_data
        """
        task_data = self.r.eval(lua, 2, TASK_QUEUE_KEY, TASK_ADDED_KEY)
        if not task_data:
            print("LabelScheduler: No new tasks, waiting...")
            return None

        task = json.loads(task_data)
        return task

    '''
    scraper放回来的时候应该left=False
    '''
    def add_task(self, task, left=True):
        lua = """
            local in_added = redis.call('SISMEMBER', KEYS[1], ARGV[2])
            local in_completed = redis.call('SISMEMBER', KEYS[2], ARGV[2])

            if in_added == 0 and in_completed == 0 then
                -- Add to queue based on left flag
                if ARGV[3] == "1" then
                    redis.call('LPUSH', KEYS[3], ARGV[1])
                else
                    redis.call('RPUSH', KEYS[3], ARGV[1])
                end
                -- Add to added set
                redis.call('SADD', KEYS[1], ARGV[2])
                return 1  -- Success
            end
            return 0  -- Not added
        """
        key = self.__key(task['label'], task['timeBucketId'])
        added = self.r.eval(lua, 3,
                   TASK_ADDED_KEY,
                   TASK_COMPLETED_KEY,
                   TASK_QUEUE_KEY,
                   json.dumps(task),
                   key,
                   "1" if left else "0")
        if added:
            print("LabelScheduler: Added new task: ", task)

    def complete_task(self, label, timeBucketId):
        self.r.sadd(TASK_COMPLETED_KEY, self.__key(label, timeBucketId))

    def init_tasks(self, labels, days_back=30):
        now = datetime.now()
        start = now - timedelta(days=days_back)
        left = True
        while start <= now:
            timeBucketId = TimeBucket.from_datetime(start).id - 1
            if left and start >= TimeBucket.from_datetime(now).id - 240:
                left = False
            for label in labels:
                if label.startswith('#'):
                    self.add_task({
                        "timeBucketId": timeBucketId,
                        "contentSizeBytes": 0,
                        "label": label,
                        "cursor": None,
                        "source": DataSource.X
                    }, left=left)
                elif label.startswith('r/'):
                    self.add_task({
                        "timeBucketId": timeBucketId,
                        "contentSizeBytes": 0,
                        "label": label,
                        "cursor": None,
                        "source": DataSource.REDDIT
                    }, left=left)
            start += timedelta(hours=1)
        print("LabelScheduler: initialize 30days tasks completed")

    def schedule_realtime_tasks(self):
        now = datetime.now()
        timeBucketId = TimeBucket.from_datetime(now).id - 1
        for label in self.labels + self.trends + self.total:
            if label.startswith('#'):
                self.add_task({
                    "timeBucketId": timeBucketId,
                    "contentSizeBytes": 0,
                    "label": label,
                    "cursor": None,
                    "source": DataSource.X
                }, left=False)
            elif label.startswith('r/'):
                self.add_task({
                    "timeBucketId": timeBucketId,
                    "contentSizeBytes": 0,
                    "label": label,
                    "cursor": None,
                    "source": DataSource.REDDIT
                }, left=False)