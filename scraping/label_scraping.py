import asyncio
from collections import deque
import requests
import datetime as dt
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.x.model import XContent
from scraping.reddit.model import RedditContent, RedditDataType
from common.data import DataEntity,TimeBucket, DataLabel, DataSource
from storage.miner.miner_storage import MinerStorage
from common.date_range import DateRange
import bittensor as bt
from scraping.label_scheduler import LabelScheduler
import threading

class SizeAwareQueue:
    """Thread-safe queue with size tracking"""
    def __init__(self, max_total_size_bytes):
        self._queue = deque()
        self._current_size = 0
        self._max_size = max_total_size_bytes
        self._lock = asyncio.Lock()
        self._size_exceeded = False
        self._count = 0

    async def put(self, chunk, chunk_size):
        async with self._lock:
            if self._size_exceeded:
                return False
            
            if self._current_size + chunk_size > self._max_size:
                self._size_exceeded = True
                return False
                
            self._queue.append(chunk)
            self._current_size += chunk_size
            self._count +=1 
            if self._count == 16:
                self._count = 0
                bt.logging.info(f"Scraped {self._current_size/1024/1024:.2f}MB data")
            return True

    async def get(self):
        async with self._lock:
            if not self._queue:
                return None
            return self._queue.popleft()

    async def should_continue(self):
        async with self._lock:
            return not self._size_exceeded

class LabelScraper:
    def __init__(
        self,
        storage: MinerStorage,
        scheduler: LabelScheduler,
        shutdown_event: threading.Event
    ):

        self.storage = storage
        self.scheduler = scheduler
        
        self._shutdown_event = shutdown_event
        
        asyncio.create_task(self._watch_shutdown())
        print("init label scraper")

    async def _watch_shutdown(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1)
            except Exception as e:
                print(f"Error: {e}")
        
        await self.handle_shutdown()


    def generate_current_hour_query(self, base_query, date_range: DateRange):
        """Generate Twitter search query for current hour only"""
        
        date_format = "%Y-%m-%d_%H:%M:%S_UTC"

        return f"since:{date_range.start.astimezone(tz=dt.timezone.utc).strftime(date_format)} until:{date_range.end.astimezone(tz=dt.timezone.utc).strftime(date_format)} ({base_query})"

    async def fetch_tweets_for_tag(
        self,
        tag: str,
        date_range: DateRange,
        output_queue: SizeAwareQueue,
        chunk_size_bytes: int,
        cursor: str|None = None
    ) :
        """Fetch tweets for a single tag in chunks"""
        scraper = ApiDojoTwitterScraper()
        bucket_id = TimeBucket.from_datetime(date_range.start).id
        query = self.generate_current_hour_query(tag, date_range)
        last_cursor = cursor
        current_chunk = []
        current_chunk_size = 0
        chunk_num = 1
        skip_total = 0
        

        # Time range filters
        # age_limit = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc) - dt.timedelta(days=30)
        current_hour_start = date_range.start
        start = dt.datetime.now()
        while await output_queue.should_continue():
            try:
                async for new_tweets, new_cursor in scraper.api.search_with_cursor(query, 1000, cursor=cursor):
                    if not await output_queue.should_continue():  # Check before processing each batch
                        bt.logging.info(f"Size limit reached during processing, stopping fetch for tag {tag} in {bucket_id}")
                        return cursor
                    cursor = new_cursor
                    x_contents, is_retweets, skip_count = scraper._best_effort_parse_tweets(new_tweets)
                    skip_total += skip_count
                    
                    data_entities :list[DataEntity] = []
                    for x_content in x_contents:
                        data_entity = XContent.to_data_entity(content=x_content)
                        if data_entity.content_size_bytes < 65520:
                            data_entities.append(data_entity)

                    for data in data_entities:
                        # Only count size for tweets in current hour with NULL first_tag
                        if data.datetime >= current_hour_start:
                            current_chunk_size += data.content_size_bytes
                        
                        current_chunk.append(data)
                        
                        # Submit chunk when size threshold reached
                        if current_chunk_size >= chunk_size_bytes:
                            end = dt.datetime.now()
                            time_diff = end -start
                            bt.logging.success(f"Scraped {len(current_chunk)} tweets in chunk {tag}-{chunk_num} , with {current_chunk_size} bytes label {tag} tweets in {bucket_id}, skip {skip_total} old age tweets, elapsed {time_diff.total_seconds():.2f}s")
                            if not await output_queue.put(current_chunk, current_chunk_size):
                                bt.logging.success(f"end of scrape {tag} in {bucket_id}")
                                return cursor
                            current_chunk = []
                            current_chunk_size = 0
                            chunk_num += 1
                            start = end
                            skip_total = 0

                if cursor == last_cursor:
                    end = dt.datetime.now()
                    time_diff = end -start
                    if len(current_chunk) > 0:
                        bt.logging.success(f"use tag {tag} scraped {len(current_chunk)} tweets in  chunk {tag}-{chunk_num} (last), with {current_chunk_size} bytes label {tag} tweets in {bucket_id}, elapsed {time_diff.total_seconds():.2f}s")
                        await output_queue.put(current_chunk, current_chunk_size)
                    bt.logging.success(f"end of scrape {tag} in {bucket_id}")
                    return None
                else: 
                    last_cursor = cursor
                self._current_task["cursor"]= cursor

            except Exception as e:
                bt.logging.error(f"Error processing tag {tag} in {bucket_id}: {str(e)}")
                if current_chunk:  # Submit collected data on error
                    end = dt.datetime.now()
                    time_diff = end -start
                    bt.logging.success(f"use tag {tag} scraped {len(current_chunk)} in  chunk {tag}-{chunk_num} (last), with {current_chunk_size} bytes label {tag} tweets in {bucket_id}, elapsed {time_diff.total_seconds():.2f}s")
                    bt.logging.success(f"end of scrape {tag} in {bucket_id}")
                    await output_queue.put(current_chunk, current_chunk_size)
                return cursor

    async def fetch_reddit_for_tag(self, tag: str, date_range: DateRange, output_queue: SizeAwareQueue, max_retries = 3):
        """Fetch Reddit posts for a single tag"""
        bucket_id = TimeBucket.from_datetime(date_range.start).id
        now = dt.datetime.now()
        age_limit = now - dt.timedelta(days=30)
        if bucket_id < TimeBucket.from_datetime(age_limit).id:
            bt.logging.warning(f"Reddit scraping for tag {tag} in {bucket_id} is limited to posts within the last 30 days, skipping older posts.")
            return
        
        """
        抓取Reddit帖子数据，并自动处理分页和错误重试
        
        :param tag: 子版块名称
        :param after: 开始时间戳
        :param before: 结束时间戳
        :param max_retries: 最大重试次数
        """
        retry_count = 0
        current_after = int(date_range.start.timestamp()) -1
        before = int(date_range.end.timestamp())
        
        while retry_count < max_retries:
            try:
                start = dt.datetime.now()
                # 构建请求URL
                url = f"https://arctic-shift.photon-reddit.com/api/posts/search?limit=auto&sort=asc&subreddit={tag}&after={current_after}&before={before}"
                
                # 发送GET请求
                response = requests.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    },
                    timeout=30
                )
                
                # 检查响应状态
                response.raise_for_status()
                
                # 解析JSON数据
                data = response.json()
                
                # 检查数据是否为空
                if len(data["data"]) == 0:
                    bt.logging.success(f"end of scrape {tag} in {bucket_id} with {output_queue._current_size} data")
                    return
                
                # 处理数据
                posts = data["data"]

                data_entities = []
                current_chunk_size = 0
                for post in posts:
                    date = dt.datetime.utcfromtimestamp(int(post["created_utc"])).replace(
                            tzinfo=dt.timezone.utc
                        )
                    content = RedditContent(
                        id=post["name"],
                        url="https://www.reddit.com"
                            + normalize_permalink(post["permalink"]),
                        username=post["author"],
                        communityName=post["subreddit_name_prefixed"],
                        body=post["selftext"] if "selftext" in post else post["body"],
                        createdAt=date,
                        dataType=RedditDataType.POST if "selftext" in post else RedditDataType.COMMENT,
                        title=post.get("title", None),
                        parentId=post.get("parent_id", None)
                    )
                    de = RedditContent.to_data_entity(content)
                    current_chunk_size += de.content_size_bytes
                    data_entities.append(de)

                end = dt.datetime.now()
                time_diff = end -start
                bt.logging.success(f"use tag {tag} scraped {len(data_entities)} reddits , with {current_chunk_size} bytes label {tag} reddits in {bucket_id}, elapsed {time_diff.total_seconds():.2f}s")

                
                if not await output_queue.put(data_entities, current_chunk_size):
                    bt.logging.success(f"end of scrape {tag} in {bucket_id} with {output_queue._current_size} data")
                    return

                last_post = posts[-1]
                new_after = last_post.get("created_utc")
                
                if not new_after:
                    retry_count += 1
                    bt.logging.error(f"cannot get last created_utc in {last_post} (retry {retry_count}/{max_retries})")
                    if retry_count < max_retries:
                        await asyncio.sleep(5 * retry_count)  # 指数退避
                    else:
                        bt.logging.error(f"exceed max retries for {url}: cannot get last created_utc in {last_post}")
                        return
                    
                current_after = new_after
                retry_count = 0  # 重置重试计数器
                
                # 避免请求过于频繁
                await asyncio.sleep(1)
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                bt.logging.error(f"request failed (retry {retry_count}/{max_retries}): {str(e)}")
                if retry_count < max_retries:
                    await asyncio.sleep(5 * retry_count)  # 指数退避
                else:
                    bt.logging.error(f"exceed max retries for {url}: {str(e)}")
                    return
            except Exception as e:
                bt.logging.error(f"Error fetching Reddit posts for tag {tag}: {str(e)}")
                return

    async def process_tweets_consumer(self,output_queue: SizeAwareQueue):
        """Consumer coroutine to process fetched tweets"""
        count = 0
        while not self.stop_event.is_set():
            chunk = await output_queue.get()
            if chunk is None:
                await asyncio.sleep(1)
                count +=1
                if count == 120:
                    count = 0
                    bt.logging.info("consumer heart beats")
                
                continue
            
            
            # Process tweet chunk (storage/analysis)
            bt.logging.success(f"Processing chunk with {len(chunk)} DataEntities")
            start = dt.datetime.now()
            try:
                self.storage.store_data_entities(chunk)
                end = dt.datetime.now()
                time_diff = end -start
                bt.logging.success(f"store {len(chunk)} DataEntities elapsed {time_diff.total_seconds():.2f}s ")
            # await save_to_db(chunk)
            except Exception as e:
                bt.logging.error("label worker error : " + str(e))
        bt.logging.info("process_tweets_consumer exit")

    async def process_tags_parallel(
        self,
        tag: str,
        bucket_id: int,
        date_range :DateRange,
        chunk_size_bytes: int = 1 *1024 *1024,
        cursor: str|None = None,
        source: int|None = None,
        max_total_size_bytes: int = 128 * 1024 * 1024,
    ):
        """Process multiple tags in parallel with size control"""
        output_queue = SizeAwareQueue(max_total_size_bytes + 2 * chunk_size_bytes)
        self.stop_event = asyncio.Event()
        self._current_task = {
            "timeBucketId": bucket_id,
            "contentSizeBytes": 0,
            "label": tag,
            "cursor": cursor,
            "source": source if source is not None else DataSource.X
        }

        new_cursor = None
        # Start consumer
        consumer_task = asyncio.create_task(self.process_tweets_consumer(output_queue))
        
        
        if source == DataSource.REDDIT:
            await self.fetch_reddit_for_tag(tag, date_range, output_queue)
        else:
            # Wait for producers to complete
            new_cursor= await self.fetch_tweets_for_tag(tag, date_range, output_queue, chunk_size_bytes, cursor)
        
        # Notify consumer to finish
        self.stop_event.set()
        await consumer_task
        
        return new_cursor

    async def handle_shutdown(self):
        """Handle shutdown signal by saving current task state."""
        bt.logging.info("Shutdown signal received, saving task state...")
        
        if hasattr(self, '_current_task'):
            # 保存当前任务状态回调度器
            bt.logging.info(f"Saved current task state: {self._current_task}")
            self.scheduler.add_task(self._current_task, left=False)
        
        # 设置运行标志为False以退出循环
        self.stop_event.set()
        return True
        

def normalize_permalink(permalink: str) -> str:
    "Ensures that the reddit permalink always starts with '/r/' prefix (including a leading /)"
    if permalink.startswith("/"):
        return permalink
    else:
        return "/" + permalink