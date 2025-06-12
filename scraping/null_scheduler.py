import redis
import simplejson as json
from datetime import datetime, timedelta
from common.data import TimeBucket

TASK_QUEUE_KEY="x:null:task_queue" # list
TASK_ADDED_KEY="x:null:task_added"  # set
TASK_COMPLETED_KEY="x:null:task_completed" # set timeBucketId

class NullScheduler:

    @classmethod
    def from_conn(cls, host: str, port: int, password: str):
        r = redis.Redis(host=host, port=port, db=0, password=password)
        return cls(r)

    def __init__(self, r):
        self.r = r

    def get_task(self):
        task_data = self.r.brpop(TASK_QUEUE_KEY, timeout=5)
        if not task_data:
            print("NullScheduler: No new tasks, waiting...")
            return None

        _, raw_task = task_data
        task = json.loads(raw_task)
        self.r.srem(TASK_ADDED_KEY, task["timeBucketId"])
        # task["timeBucketId"] task["contentSizeBytes"] task["cursor"]
        return task

    '''
    null scraper放回来的时候应该left=False
    '''
    def add_task(self, task, left=True):
        timeBucketId = task["timeBucketId"]
        if not self.r.sismember(TASK_ADDED_KEY, timeBucketId) and not self.r.sismember(TASK_COMPLETED_KEY, timeBucketId):
            if left:
                self.r.lpush(TASK_QUEUE_KEY, json.dumps(task))
            else:
                self.r.rpush(TASK_QUEUE_KEY, json.dumps(task))
            self.r.sadd(TASK_ADDED_KEY, timeBucketId)
            print("NullScheduler: Added new task: ", task)

    def complete_task(self, timeBucketId):
        self.r.sadd(TASK_COMPLETED_KEY, timeBucketId)

    def init_tasks(self, days_back=30):
        now = datetime.now()
        start = now - timedelta(days=days_back)
        while start < now:
            timeBucketId = TimeBucket.from_datetime(start).id - 1
            self.add_task({
                "timeBucketId": timeBucketId,
                "contentSizeBytes": 0,
                "tag": "a",
                "cursor": None
            }, left=False)
            start += timedelta(hours=1)
        print("NullScheduler: initialize 30days tasks completed")

    def schedule_realtime_tasks(self):
        now = datetime.now()
        timeBucketId = TimeBucket.from_datetime(now).id - 1
        self.add_task({
            "timeBucketId": timeBucketId,
            "contentSizeBytes": 0,
            "tag": "a",
            "cursor": None
        }, left=False)