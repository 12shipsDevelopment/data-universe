import redis
import simplejson as json
from datetime import datetime, timedelta
from common.data import TimeBucket

TASK_QUEUE_KEY="x:label:task_queue" # list
TASK_ADDED_KEY="x:label:task_added"  # set
TASK_COMPLETED_KEY="x:label:task_completed" # set timeBucketId

class LabelScheduler:
    # @classmethod
    # def from_conn(cls, host: str, port: int, password: str):
    #     r = redis.Redis(host=host, port=port, db=0, password=password)
    #     return cls(r)

    def __init__(self, r, labels = []):
        self.r = r
        self.labels = labels

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
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
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
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        timeBucketId = TimeBucket.from_datetime(now).id - 1
        for label in self.labels:
            self.add_task({
                "timeBucketId": timeBucketId,
                "contentSizeBytes": 0,
                "label": label,
                "cursor": None
            }, left=False)