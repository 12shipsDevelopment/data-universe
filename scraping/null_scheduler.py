import redis
import json
import time
from datetime import datetime, timedelta
from common.data import TimeBucket
import asyncio

TASK_QUEUE_KEY="x:null:task_queue" # list
TASK_ADDED_KEY="x:null:task_added"  # set
TASK_COMPLETED_KEY="x:null:task_completed" # set timeBucketId

class NullScheduler:
    def __init__(self, host: str, port: int, password: str):
        self.r = redis.Redis(host=host, port=port, db=0, password=password)

    def get_task(self):
        task_data = self.r.brpop(TASK_QUEUE_KEY, timeout=5)
        if not task_data:
            print("No new tasks, waiting...")
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
            print("Added new task: ", task)

    def complete_task(self, timeBucketId):
        self.r.sadd(TASK_COMPLETED_KEY, timeBucketId)

    def init_tasks(self, days_back=30):
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
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
        print("initialize 30days tasks completed")

    def schedule_realtime_tasks(self):
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        timeBucketId = TimeBucket.from_datetime(now).id - 1
        self.add_task({
            "timeBucketId": timeBucketId,
            "contentSizeBytes": 0,
            "tag": "a",
            "cursor": None
        }, left=False)