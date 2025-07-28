import redis
import simplejson as json
from datetime import datetime, timedelta
from common.data import TimeBucket, DataSource

TASK_COMPLETED_KEY="x:label:task_completed" # set timeBucketId

X_QUEUE_KEY="x:label:task_queue" # list
X_ADDED_KEY="x:label:task_added"  # set

def read_default_labels():
    try:
        with open("scraping/default_twitter_labels.json", "r") as f:
            labels = json.load(f)
            if isinstance(labels, list):
                return labels
    except FileNotFoundError:
        pass
    return []

class TwitterScheduler:
    @classmethod
    def from_conn(cls, host: str, port: int, password: str):
        r = redis.Redis(host=host, port=port, db=0, password=password)
        return cls(r)

    def __init__(self, r):
        self.r = r
        self.labels = read_default_labels()
        print(f"load {len(self.labels)} labels from default_labels.json")
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
        task_data = self.r.eval(lua, 2, X_QUEUE_KEY, X_ADDED_KEY)

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
        if task["source"] == DataSource.X:
            added = self.r.eval(lua, 3,
                X_ADDED_KEY,
                TASK_COMPLETED_KEY,
                X_QUEUE_KEY,
                json.dumps(task),
                key,
                "1" if left else "0")

        if added:
            print("LabelScheduler: Added new task: ", task)

    def add_retrive_task(self, task):
        task_data = json.dumps(task)
        self.r.lpush(X_QUEUE_KEY,task_data)

    def complete_task(self, label, timeBucketId):
        self.r.sadd(TASK_COMPLETED_KEY, self.__key(label, timeBucketId))

    def init_tasks(self, labels, days_back=30):
        now = datetime.now()
        start = now - timedelta(days=days_back)
        left_boundary = now - timedelta(days=10)
        left = True
        while start <= now:
            timeBucketId = TimeBucket.from_datetime(start).id - 1
            if left and start >= left_boundary:
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
