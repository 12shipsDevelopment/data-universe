import asyncio
from collections import deque
from datetime import datetime, timedelta
import datetime as dt
from twscrape import API
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.x.model import XContent
from common.data import DataEntity
from storage.miner.miner_storage import MinerStorage
from common.date_range import DateRange
import bittensor as bt

class SizeAwareQueue:
    """Thread-safe queue with size tracking"""
    def __init__(self, max_total_size_bytes):
        self._queue = deque()
        self._current_size = 0
        self._max_size = max_total_size_bytes
        self._lock = asyncio.Lock()
        self._size_exceeded = False
        self._shutdown = False  # New flag for shutdown

    async def put(self, chunk, chunk_size):
        async with self._lock:
            if self._shutdown or self._size_exceeded:
                return False
            
            if self._current_size + chunk_size > self._max_size:
                self._size_exceeded = True
                bt.logging.info("Queue size limit reached")
                return False
                
            self._queue.append(chunk)
            self._current_size += chunk_size
            return True

    async def get(self):
        async with self._lock:
            if not self._queue:
                return None
            return self._queue.popleft()

    async def should_continue(self):
        async with self._lock:
            return not (self._size_exceeded or self._shutdown)
    
    async def shutdown(self):
        async with self._lock:
            self._shutdown = True
            bt.logging.info("Queue shutdown initiated")

def generate_current_hour_query(base_query, date_range: DateRange):
    """Generate Twitter search query for current hour only"""
    date_format = "%Y-%m-%d_%H:%M:%S_UTC"
    return f"since:{date_range.start.astimezone(tz=dt.timezone.utc).strftime(date_format)} until:{date_range.end.astimezone(tz=dt.timezone.utc).strftime(date_format)} ({base_query})"

async def fetch_tweets_for_tag(
    tag: str,
    date_range: DateRange,
    output_queue: SizeAwareQueue,
    chunk_size_bytes: int
):
    """Fetch tweets for a single tag in chunks"""
    scraper = ApiDojoTwitterScraper()
    query = generate_current_hour_query(tag, date_range)
    cursor = None
    current_chunk = []
    current_chunk_size = 0
    chunk_num = 1
    skip_total = 0
    
    current_hour_start = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc) - dt.timedelta(hours=1)
    start = dt.datetime.now()
    
    try:
        while await output_queue.should_continue():
            try:
                async for new_tweets, new_cursor in scraper.api.search_with_cursor(query, 1000, cursor=cursor):
                    if not await output_queue.should_continue():
                        bt.logging.info(f"Stopping tag {tag} due to queue shutdown")
                        return
                        
                    cursor = new_cursor
                    x_contents, is_retweets, skip_count = scraper._best_effort_parse_tweets(new_tweets)
                    skip_total += skip_count
                    
                    data_entities: list[DataEntity] = []
                    for x_content in x_contents:
                        data_entities.append(XContent.to_data_entity(content=x_content))

                    for data in data_entities:
                        if data.datetime >= current_hour_start and not data.label:
                            current_chunk_size += data.content_size_bytes
                        current_chunk.append(data)
                        
                        if current_chunk_size >= chunk_size_bytes:
                            end = dt.datetime.now()
                            time_diff = end - start
                            bt.logging.success(f"Tag {tag}: Scraped {len(current_chunk)} tweets in chunk {chunk_num}, {current_chunk_size} bytes null tag tweets, skipped {skip_total} old tweets, elapsed {time_diff.total_seconds():.2f}s")
                            if not await output_queue.put(current_chunk, current_chunk_size):
                                return
                            current_chunk = []
                            current_chunk_size = 0
                            chunk_num += 1
                            start = end
                            skip_total = 0

                    if not cursor and current_chunk:
                        end = dt.datetime.now()
                        time_diff = end - start
                        bt.logging.success(f"Tag {tag}: Final chunk with {len(current_chunk)} tweets, {current_chunk_size} bytes null tag tweets, elapsed {time_diff.total_seconds():.2f}s")
                        await output_queue.put(current_chunk, current_chunk_size)
                        return

            except Exception as e:
                bt.logging.error(f"Error processing tag {tag}: {str(e)}")
                if current_chunk:
                    end = dt.datetime.now()
                    time_diff = end - start
                    bt.logging.info(f"Tag {tag}: Error chunk with {len(current_chunk)} tweets, {current_chunk_size} bytes null tag tweets, elapsed {time_diff.total_seconds():.2f}s")
                    await output_queue.put(current_chunk, current_chunk_size)
                return
    except asyncio.CancelledError:
        bt.logging.info(f"Tag {tag} task cancelled")
        raise

async def process_tweets_consumer(output_queue: SizeAwareQueue, storage: MinerStorage):
    """Consumer coroutine to process fetched tweets"""
    processed_count = 0
    start_time = dt.datetime.now()
    
    try:
        while True:
            chunk = await output_queue.get()
            if chunk is None:
                if not await output_queue.should_continue():
                    bt.logging.info("Consumer stopping - queue shutdown requested")
                    break
                await asyncio.sleep(0.1)
                continue
            
            bt.logging.success(f"Processing chunk with {len(chunk)} DataEntities (total processed: {processed_count})")
            chunk_start = dt.datetime.now()
            try:
                storage.store_data_entities(chunk)
                processed_count += len(chunk)
                chunk_end = dt.datetime.now()
                time_diff = chunk_end - chunk_start
                bt.logging.success(f"Stored {len(chunk)} DataEntities in {time_diff.total_seconds():.2f}s (total: {processed_count})")
            except Exception as e:
                bt.logging.error(f"Storage error: {str(e)}")
    except asyncio.CancelledError:
        total_time = (dt.datetime.now() - start_time).total_seconds()
        bt.logging.info(f"Consumer task cancelled. Processed {processed_count} items in {total_time:.2f}s")
        raise
    finally:
        total_time = (dt.datetime.now() - start_time).total_seconds()
        bt.logging.info(f"Consumer completed. Processed {processed_count} items in {total_time:.2f}s")

async def process_tags_parallel(
    tags: list,
    date_range: DateRange,
    storage: MinerStorage,
    max_total_size_bytes: int = 128 * 1024 * 1024,
    parallel_tasks: int = 5,
    chunk_size_bytes: int = 1 * 1024 * 1024,
    timeout_minutes: int = 30
):
    """Process multiple tags in parallel with size control"""
    output_queue = SizeAwareQueue(max_total_size_bytes + 2 * chunk_size_bytes)
    
    try:
        # Start consumer
        consumer_task = asyncio.create_task(process_tweets_consumer(output_queue, storage))
        
        # Start producers with timeout
        producers = []
        semaphore = asyncio.Semaphore(parallel_tasks)
        
        async def limited_worker(tag):
            async with semaphore:
                try:
                    await asyncio.wait_for(
                        fetch_tweets_for_tag(tag, date_range, output_queue, chunk_size_bytes),
                        timeout=timeout_minutes * 60
                    )
                except asyncio.TimeoutError:
                    bt.logging.warning(f"Tag {tag} processing timed out after {timeout_minutes} minutes")
                except Exception as e:
                    bt.logging.error(f"Error in tag {tag} worker: {str(e)}")
        
        # Create producer tasks
        for tag in tags:
            if not await output_queue.should_continue():
                bt.logging.info("Stopping early - queue limit reached")
                break
            producers.append(asyncio.create_task(limited_worker(tag)))
        
        # Wait for producers to complete or timeout
        try:
            await asyncio.wait_for(asyncio.gather(*producers), timeout=timeout_minutes * 60)
        except asyncio.TimeoutError:
            bt.logging.warning(f"Overall processing timed out after {timeout_minutes} minutes")
        
        # Shutdown sequence
        bt.logging.info("Initiating shutdown sequence...")
        await output_queue.shutdown()
        
        # Give consumer time to finish processing
        try:
            await asyncio.wait_for(consumer_task, timeout=5)
        except asyncio.TimeoutError:
            bt.logging.warning("Consumer did not finish in time, cancelling...")
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
        
        return output_queue._current_size
        
    except Exception as e:
        bt.logging.error(f"Error in process_tags_parallel: {str(e)}")
        raise
    finally:
        # Clean up any remaining tasks
        for task in producers:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass