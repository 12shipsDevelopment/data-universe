import asyncio
from collections import deque
from datetime import datetime, timedelta
import datetime as dt
from twscrape import API
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.x.model import XContent
from common.data import DataEntity,TimeBucket
from storage.miner.miner_storage import MinerStorage
from common.date_range import DateRange
import bittensor as bt
from scraping.null_scheduler import NullScheduler
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

class NullScraper:
    def __init__(
        self,
        storage: MinerStorage,
        scheduler: NullScheduler,
        shutdown_event: threading.Event
    ):

        self.storage = storage
        self.scheduler = scheduler
        
        self._shutdown_event = shutdown_event
        
        asyncio.create_task(self._watch_shutdown())
        print("init null scraper")

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
                        if data_entity.content_size_bytes < 65520: # size limit for data entity
                            data_entities.append(data_entity)

                    for data in data_entities:
                        # Only count size for tweets in current hour with NULL first_tag
                        if data.datetime >= current_hour_start:
                            if not data.label:  # NULL tag
                                current_chunk_size += data.content_size_bytes
                        
                        current_chunk.append(data)
                        
                        # Submit chunk when size threshold reached
                        if current_chunk_size >= chunk_size_bytes:
                            end = dt.datetime.now()
                            time_diff = end -start
                            bt.logging.success(f"Scraped {len(current_chunk)} tweets in chunk {tag}{chunk_num} , with {current_chunk_size} bytes null tag tweets in {bucket_id}, skip {skip_total} old age tweets, elapsed {time_diff.total_seconds():.2f}s")
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
                        bt.logging.success(f"use tag {tag} scraped {len(current_chunk)} tweets in  chunk {tag}{chunk_num} (last), with {current_chunk_size} bytes null tag tweets in {bucket_id}, elapsed {time_diff.total_seconds():.2f}s")
                        await output_queue.put(current_chunk, current_chunk_size)
                    bt.logging.success(f"end of scrape {tag} in {bucket_id}")
                    return None
                else: 
                    last_cursor = cursor
                self._current_task["cursor"]= cursor

            except Exception as e:
                bt.logging.error(f"Error processing tag {tag}: {str(e)}")
                if current_chunk:  # Submit collected data on error
                    end = dt.datetime.now()
                    time_diff = end -start
                    bt.logging.success(f"use tag {tag} scraped {len(current_chunk)} in  chunk {tag}{chunk_num} (last), with {current_chunk_size} bytes null tag tweets in {bucket_id}, elapsed {time_diff.total_seconds():.2f}s")
                    bt.logging.success(f"end of scrape {tag} in {bucket_id}")
                    await output_queue.put(current_chunk, current_chunk_size)
                return cursor

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
                bt.logging.error("null worker error: " + str(e))
        bt.logging.info("process_tweets_consumer exit")

    async def process_tags_parallel(
        self,
        tag: str,
        bucket_id: int,
        date_range :DateRange,
        max_total_size_bytes: int = 128 * 1024 * 1024,
        chunk_size_bytes: int = 1 *1024 *1024,
        cursor: str|None = None,
    ):
        """Process multiple tags in parallel with size control"""
        output_queue = SizeAwareQueue(max_total_size_bytes + 2 * chunk_size_bytes)
        self.stop_event = asyncio.Event()  
        self._current_task = {
            "timeBucketId": bucket_id,
            "contentSizeBytes": 0,
            "tag": tag,
            "cursor": cursor
        }

        # Start consumer
        consumer_task = asyncio.create_task(self.process_tweets_consumer(output_queue))
        
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
        