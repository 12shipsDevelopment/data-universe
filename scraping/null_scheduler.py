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
        lua = """
        local task = redis.call('RPOP', KEYS[1])
        if task then
            local obj = cjson.decode(task)
            redis.call('SREM', KEYS[2], obj.timeBucketId)
        return task
        """
        task_data = self.r.eval(lua, 2, TASK_QUEUE_KEY, TASK_ADDED_KEY)
        if not task_data:
            print("NullScheduler: No new tasks, waiting...")
            return None

        _, raw_task = task_data
        task = json.loads(raw_task)
        return task

    '''
    null scraper放回来的时候应该left=False
    '''
    def add_task(self, task, left=True):
        lua = """
            local task = cjson.decode(ARGV[1])
            local timeBucketId = task.timeBucketId
            local left = tonumber(ARGV[2]) == 1  -- 1 for true, 0 for false

            -- Check if timeBucketId exists in either set
            local inAdded = redis.call('SISMEMBER', KEYS[1], timeBucketId)
            local inCompleted = redis.call('SISMEMBER', KEYS[2], timeBucketId)

            if inAdded == 0 and inCompleted == 0 then
                -- Add to queue based on 'left' flag
                if left == 1 then
                    redis.call('LPUSH', KEYS[3], ARGV[1])
                else
                    redis.call('RPUSH', KEYS[3], ARGV[1])
                end
                -- Add to added set
                redis.call('SADD', KEYS[1], timeBucketId)
                return 1  -- Indicate task was added
            end
            return 0  -- Indicate task was not added
        """
        added = self.r.eval(lua, 3, 
                   TASK_ADDED_KEY, 
                   TASK_COMPLETED_KEY, 
                   TASK_QUEUE_KEY,
                   json.dumps(task),
                   1 if left else 0)
        if added:
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