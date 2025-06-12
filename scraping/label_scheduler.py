import redis
import simplejson as json
from datetime import datetime, timedelta
from common.data import TimeBucket

TASK_QUEUE_KEY="x:label:task_queue" # list
TASK_ADDED_KEY="x:label:task_added"  # set
TASK_COMPLETED_KEY="x:label:task_completed" # set timeBucketId

DEFAULT_LABELS = [
    "#riyadh",
    "#сryрtо",
    "#pr",
    "#tiktok",
    "#jeddah",
    "#ad",
    "#lolfanfest2025d2",
    "#lolfanfest2025d1",
    "#ピッコマ",
    "#yahooニュース",
    "#انقاذ_النصر_يالصندوق",
    "#lorealparisworthitthxmilklove",
    "#goスト",
    "#btc",
    "#xrp",
    "#enhypen",
    "#bitcoin",
    "#sb19",
    "#hatchonbnb",
    "#whalestorexoxo",
    "#fogochain",
    "#lacasadelosfamososcol",
    "#sоnic",
    "#staywithusosimhen",
    "#standagainstninestar",
    "#aiforall",
    "#granhermano",
    "#ハチミツ争奪戦",
    "#terstegenout",
    "#riyadhseason",
    "#grabfoodลดหั่นครึ่งxwilliamest",
    "#wearemadleen",
    "#เน็ตฟลิกซ์ราคาถูก",
    "#zonauang",
    "#แจกจริง",
    "#mygoldenbloodfinalep",
    "#おはようvtuber",
    "#تدخل_الصندوق_مطلب_نصراوي",
    "#भगवान_के_चश्मदीद_गवाह",
    "#sixtonesann",
    "#trump",
    "#lovefighters",
    "#阪神タイガース",
    "#gold",
    "#gquuuuuux",
    "#परमात्मा_न_जन्मताहै_न_मरताहै",
    "#tiktoklite",
    "#seventeen",
    "#amazon",
    "#emdistrictxwilliamest",
    "#تدخل_الصندوق_مطلب_نصراوي5",
    "#महापापी_जो_करे_जीवहिंसा",
    "#mygoldenbloodep11",
    "#crypto",
    "#金魚すくいバトル",
    "#ดูดวง",
    "#iriam",
    "#निमंत्रणसंसारको_सम्मानकेसाथ",
    "#परमात्मा_की_पहचान",
    "#خادم_حرم_ایران",
    "#अध्यात्म_का_बेड़ा_गर्क_करदिया",
    "#lazada66ลดจริงกว่าใคร",
    "#eurovision2025",
    "#lorealxbeckyincannes2025",
    "#nosaint_everate_meat",
    "#whois_kaal_inbhagavadgita",
    "#virτυàls",
    "#सतभक्ति_से_ऐसेसुखमिलताहै",
    "#マシュマロを投げ合おう",
    "#thenextprinceep3",
    "#भगवदगीता_पर_ज़बरदस्त_बहस",
    "#انقذو_النصر_من_ادارته",
    "#sbhawks",
    "#يوم_عرفة",
    "#fayeperayaxpiaget",
    "#نحن_مع_غزة",
    "#olympopday",
    "#ジークアクス",
    "#baystars",
    "#happybirthdaytolingorm",
    "#eurovision",
    "#امام_امید",
    "#thenextprinceep4",
    "#モンスト",
    "#ラヴィット",
    "#lolfanfest2025live",
    "#それスノ",
    "#cometobesiktasronaldo",
    "#newprofilepic",
    "#นาฏราชครั้งที่16",
    "#wϲt",
    "#thenextprinceep6",
    "#lcdlfallstars",
    "#lingorm3rdfanmeetinhk",
    "#mygoldenbloodep10",
    "#コレコレ",
    "#大河べらぼう",
    "#مدبرامیر",
    "#leapdayseriesep9",
    "#TAO",
    "#$TAO",
    "#Bittensor",
    "#DecentralizedAI",
    "#DeAI",
    "#Near"
]

class LabelScheduler:
    @classmethod
    def from_conn(cls, host: str, port: int, password: str):
        r = redis.Redis(host=host, port=port, db=0, password=password)
        return cls(r)

    def __init__(self, r, labels :list[str] = DEFAULT_LABELS):
        self.r = r
        self.labels = labels
        self.labels.reverse()

    def __key(self, label, bucketId):    
        return f"{bucketId}-{label}"

    def get_task(self):
        task_data = self.r.brpop(TASK_QUEUE_KEY, timeout=5)
        if not task_data:
            print("LabelScheduler: No new tasks, waiting...")
            return None

        _, raw_task = task_data
        task = json.loads(raw_task)
        self.r.srem(TASK_ADDED_KEY, self.__key(task["label"], task["timeBucketId"]))
        # task["timeBucketId"] task["contentSizeBytes"] task["cursor"]
        return task

    '''
    scraper放回来的时候应该left=False
    '''
    def add_task(self, task, left=True):
        timeBucketId = task["timeBucketId"]
        label = task["label"]
        key = self.__key(label, timeBucketId)
        if not self.r.sismember(TASK_ADDED_KEY, key) and not self.r.sismember(TASK_COMPLETED_KEY, key):
            if left:
                self.r.lpush(TASK_QUEUE_KEY, json.dumps(task))
            else:
                self.r.rpush(TASK_QUEUE_KEY, json.dumps(task))
            self.r.sadd(TASK_ADDED_KEY, key)
            print("LabelScheduler: Added new task: ", task)

    def complete_task(self, label, timeBucketId):
        self.r.sadd(TASK_COMPLETED_KEY, self.__key(label, timeBucketId))

    def init_tasks(self, days_back=30):
        now = datetime.now()
        start = now - timedelta(days=days_back)
        while start < now:
            timeBucketId = TimeBucket.from_datetime(start).id - 1
            for label in self.labels:
                self.add_task({
                    "timeBucketId": timeBucketId,
                    "contentSizeBytes": 0,
                    "label": label,
                    "cursor": None
                }, left=False)
            start += timedelta(hours=1)
        print("LabelScheduler: initialize 30days tasks completed")

    def schedule_realtime_tasks(self):
        now = datetime.now()
        timeBucketId = TimeBucket.from_datetime(now).id - 1
        for label in self.labels:
            self.add_task({
                "timeBucketId": timeBucketId,
                "contentSizeBytes": 0,
                "label": label,
                "cursor": None
            }, left=False)