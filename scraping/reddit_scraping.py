import asyncio
from collections import deque
import requests
import datetime as dt
from scraping.reddit.model import RedditContent, RedditDataType
from scraping.reddit.reddit_custom_scraper import extract_media_urls2
from storage.miner.miner_storage import MinerStorage
import bittensor as bt
from scraping.reddit_scheduler import RedditScheduler
import os

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
        
    async def get_queue_size(self):
        """Get the current size of the queue"""
        async with self._lock:
            return len(self._queue)

class RedditScraper:
    def __init__(
        self,
        storage: MinerStorage,
        scheduler: RedditScheduler
    ):

        self.storage = storage
        self.scheduler = scheduler
    
        print("init label scraper")

    async def fetch_reddit_for_tag(self, tag: str, before: int, after: int, sort:str, type: str, is_nsfw: bool, output_queue: SizeAwareQueue, max_retries = 3):
        """Fetch Reddit posts for a single tag"""
        proxy = os.getenv("TWS_PROXY")

        """
        抓取Reddit帖子数据，并自动处理分页和错误重试
        
        :param tag: 子版块名称
        :param after: 开始时间戳
        :param before: 结束时间戳
        :param max_retries: 最大重试次数
        """
        retry_count = 0
        while retry_count < max_retries:
            try:
                start = dt.datetime.now()
                # 构建请求URL
                url = f"https://arctic-shift.photon-reddit.com/api/{type}/search?limit=auto&sort={sort}&subreddit={tag}&after={after}&before={before}"
                
                # 发送GET请求
                proxies = {}
                if proxy is not None:
                    proxies = {
                        'http': proxy,
                        'https': proxy
                    }
                    
                response = requests.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    },
                    timeout=30,
                    proxies=proxies
                )
                
                # 检查响应状态
                response.raise_for_status()
                
                # 解析JSON数据
                data = response.json()
                
                # 处理数据
                posts = data["data"]

                data_entities = []
                delete_urls = []
                current_chunk_size = 0
                limit = 0
                for post in posts:
                    limit += 1
                    if post.get("_meta", None):
                        if post["_meta"].get("removal_type", None):
                            removal_type = post["_meta"]["removal_type"]
                            bt.logging.info(f"post {post['name']} is removed {removal_type}, skipping")
                            permalink = normalize_permalink(post["permalink"])
                            uri = f"https://www.reddit.com{permalink}"
                            created = dt.datetime.utcfromtimestamp(int(post["created_utc"])).replace(
                                tzinfo=dt.timezone.utc
                            )
                            delete_urls.append((uri,created))
                            continue
                        elif post["_meta"].get("is_edited", False):
                            bt.logging.info(f"post {post['name']} is edited , skipping")
                            permalink = normalize_permalink(post["permalink"])
                            uri = f"https://www.reddit.com{permalink}"
                            created = dt.datetime.utcfromtimestamp(int(post["created_utc"])).replace(
                                tzinfo=dt.timezone.utc
                            )
                            delete_urls.append((uri,created))
                            continue

                    date = dt.datetime.utcfromtimestamp(int(post["created_utc"])).replace(
                            tzinfo=dt.timezone.utc
                        )
                    if type == "posts":
                        media = extract_media_urls2(post)
                        if media is not None and is_nsfw:
                            continue
                    else:
                        media = None
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
                        parentId=post.get("parent_id", None),
                        media=media,
                        is_nsfw=is_nsfw,
                        # score=post.get("score", None),
                        # upvote_ratio=post.get("upvote_ratio", None),
                        score=None,
                        upvote_ratio=None,
                        num_comments=post.get("num_comments", None)
                    )
                    de = RedditContent.to_data_entity(content)
                    if limit <= 5:
                        bt.logging.success(f"scraped {de.url}...")
                    current_chunk_size += de.content_size_bytes
                    data_entities.append(de)

                end = dt.datetime.now()
                time_diff = end -start
                bt.logging.success(f"use tag {tag} scraped {len(data_entities)} reddits , with {current_chunk_size} bytes label {tag} reddits, elapsed {time_diff.total_seconds():.2f}s")

                
                if current_chunk_size != 0:
                    if not await output_queue.put((data_entities,delete_urls), 0):
                        bt.logging.success(f"end of scrape {tag} with {current_chunk_size} data")
                        return None

                
                # 检查数据是否为空
                if len(posts) == 0:
                    return None

                last_post = posts[-1]
                last_created = last_post.get("created_utc")
                
                return last_created
                
            except requests.exceptions.RequestException as e:
                retry_count += 1
                bt.logging.error(f"request failed (retry {retry_count}/{max_retries}): {str(e)}")
                if retry_count < max_retries:
                    await asyncio.sleep(5 * retry_count)  # 指数退避
                else:
                    bt.logging.error(f"exceed max retries for {url}: {str(e)}")
                    return None
            except Exception as e:
                bt.logging.error(f"Error fetching Reddit posts for tag {tag}: {str(e)}")
                return None

    async def process_reddit_consumer(self,output_queue: SizeAwareQueue):
        """Consumer coroutine to process fetched tweets"""
        count = 0
        while not self.stop_event.is_set() or await output_queue.get_queue_size() > 0:
            chunk = await output_queue.get()
            if chunk is None:
                await asyncio.sleep(1)
                count +=1
                if count == 120:
                    count = 0
                    bt.logging.info("consumer heart beats")
                
                continue
            
            
            # Process tweet chunk (storage/analysis)
            bt.logging.success(f"Processing chunk with {len(chunk[0])} DataEntities, and delete {len(chunk[1])} removed/edited data")
            start = dt.datetime.now()
            try:
                self.storage.insert_or_delete_data_entities(chunk[0],chunk[1])
                end = dt.datetime.now()
                time_diff = end -start
                bt.logging.success(f"store {len(chunk)} DataEntities elapsed {time_diff.total_seconds():.2f}s ")
            # await save_to_db(chunk)
            except Exception as e:
                bt.logging.error("label worker error : " + str(e))
        bt.logging.info("process_reddit_consumer exit")

    async def process_tags_parallel(
        self,
        task,
        index,
        chunk_size_bytes: int = 1 *1024 *1024,
        max_total_size_bytes: int = 1024 * 1024 * 1024,
    ):
        """Process multiple tags in parallel with size control"""
        output_queue = SizeAwareQueue(max_total_size_bytes + 2 * chunk_size_bytes)
        self.stop_event = asyncio.Event()
        # Start consumer
        consumer_task = asyncio.create_task(self.process_reddit_consumer(output_queue))
        bt.logging.info(f"processing reddit task {task}")
        
        now = dt.datetime.now(tz = dt.timezone.utc)
        now_timestamp = int(now.timestamp())
        old_limit = now - dt.timedelta(days = 30)
        old_timestamp = int(old_limit.timestamp())
        retrieve_timestamp = int((now - dt.timedelta(hours = 36, minutes= 10)).timestamp())

        if task["post_before"] > old_timestamp:
            timestamp = await self.fetch_reddit_for_tag(task["label"], task["post_before"], old_timestamp, "desc", "posts", task["is_nsfw"] , output_queue)
            if timestamp is not None:
                task["post_before"] = timestamp

        timestamp = await self.fetch_reddit_for_tag(task["label"], retrieve_timestamp, task["post_after"], "asc", "posts", task["is_nsfw"] , output_queue)
        if timestamp is not None:
            task["post_after"] = timestamp

        if task["post_latest"] == 0:
            after = retrieve_timestamp
        else:
            after = task["post_latest"]
        timestamp = await self.fetch_reddit_for_tag(task["label"], now_timestamp, after, "asc", "posts", task["is_nsfw"] , output_queue)
        if timestamp is not None:
            task["post_latest"] = timestamp


        if task["comment_before"] > old_timestamp:
            timestamp = await self.fetch_reddit_for_tag(task["label"], task["comment_before"], old_timestamp, "desc", "comments", task["is_nsfw"] , output_queue)
            if timestamp is not None:
                task["comment_before"] = timestamp

        timestamp = await self.fetch_reddit_for_tag(task["label"], retrieve_timestamp, task["comment_after"], "asc", "comments", task["is_nsfw"] , output_queue)
        if timestamp is not None:
            task["comment_after"] = timestamp

        if task["comment_latest"] == 0:
            after = retrieve_timestamp
        else:
            after = task["comment_latest"]
        timestamp = await self.fetch_reddit_for_tag(task["label"], now_timestamp, after, "asc", "comments", task["is_nsfw"] , output_queue)
        if timestamp is not None:
            task["comment_latest"] = timestamp
        
        # Notify consumer to finish
        await asyncio.sleep(2)
        self.stop_event.set()
        await consumer_task

        bt.logging.info(f"finished reddit task {task}")
        self.scheduler.update_task(task,index) 
        
        return 

def normalize_permalink(permalink: str) -> str:
    "Ensures that the reddit permalink always starts with '/r/' prefix (including a leading /)"
    if permalink.startswith("/"):
        return permalink
    else:
        return "/" + permalink