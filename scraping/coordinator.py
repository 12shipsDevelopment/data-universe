import asyncio
import functools
import random
import threading
import traceback
import os
import bittensor as bt
import datetime as dt
import signal
from typing import Dict, List, Optional
import numpy
from pydantic import Field, PositiveInt, ConfigDict
from common.date_range import DateRange
from common.data import DataLabel, DataSource, StrictBaseModel, TimeBucket, DataEntityBucketId
from scraping.provider import ScraperProvider
from scraping.scraper import ScrapeConfig, ScraperId
from storage.miner.miner_storage import MinerStorage
from twscrape import AccountsPool, API
from scraping.null_scraping import NullScraper
from scraping.null_scheduler import NullScheduler


class LabelScrapingConfig(StrictBaseModel):
    """Describes what labels to scrape."""

    label_choices: Optional[List[DataLabel]] = Field(
        description="""The collection of labels to choose from when performing a scrape.
        On a given scrape, 1 label will be chosen at random from this list.

        If the list is None, the scraper will scrape "all".
        """
    )

    max_age_hint_minutes: int = Field(
        description="""The maximum age of data that this scrape should fetch. A random TimeBucket (currently hour block),
        will be chosen within the time frame (now - max_age_hint_minutes, now), using a probality distribution aligned
        with how validators score data freshness.

        Note: not all data sources provide date filters, so this property should be thought of as a hint to the scraper, not a rule.
        """,
    )

    max_data_entities: Optional[PositiveInt] = Field(
        default=None,
        description="The maximum number of items to fetch in a single scrape for this label. If None, the scraper will fetch as many items possible.",
    )


class ScraperConfig(StrictBaseModel):
    """Describes what to scrape for a Scraper."""

    cadence_seconds: PositiveInt = Field(
        description="Configures how often to scrape with this scraper, measured in seconds."
    )

    labels_to_scrape: List[LabelScrapingConfig] = Field(
        description="""Describes the type of data to scrape with this scraper.

        The scraper will perform one scrape per entry in this list every 'cadence_seconds'.
        """
    )


class CoordinatorConfig(StrictBaseModel):
    """Informs the Coordinator how to schedule scrapes."""

    scraper_configs: Dict[ScraperId, ScraperConfig] = Field(
        description="The configs for each scraper."
    )


def _choose_scrape_configs(
        scraper_id: ScraperId, config: CoordinatorConfig, now: dt.datetime
) -> List[ScrapeConfig]:
    """For the given scraper, returns a list of scrapes (defined by ScrapeConfig) to be run."""
    assert (
            scraper_id in config.scraper_configs
    ), f"Scraper Id {scraper_id} not in config"

    # Ensure now has timezone information
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)

    scraper_config = config.scraper_configs[scraper_id]
    results = []

    for label_config in scraper_config.labels_to_scrape:
        # First, choose a label
        labels_to_scrape = None
        if label_config.label_choices:
            labels_to_scrape = [random.choice(label_config.label_choices)]

        # Get max age from config or use default
        max_age_minutes = label_config.max_age_hint_minutes

        # For YouTube transcript scraper, use a wider date range
        if scraper_id == ScraperId.YOUTUBE_TRANSCRIPT:
            # Calculate the start time using max_age_minutes
            start_time = now - dt.timedelta(minutes=max_age_minutes)

            # Ensure start_time has timezone information
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=dt.timezone.utc)

            date_range = DateRange(start=start_time, end=now)

            bt.logging.info(f"Created special date range for YouTube: {date_range.start} to {date_range.end}")
            bt.logging.info(f"Date range span: {(date_range.end - date_range.start).total_seconds() / 3600} hours")

            results.append(
                ScrapeConfig(
                    entity_limit=label_config.max_data_entities,
                    date_range=date_range,
                    labels=labels_to_scrape,
                )
            )
        else:
            # For other scrapers, use the normal time bucket approach
            current_bucket = TimeBucket.from_datetime(now)
            oldest_bucket = TimeBucket.from_datetime(
                now - dt.timedelta(minutes=max_age_minutes)
            )

            chosen_bucket = current_bucket
            # If we have more than 1 bucket to choose from, choose a bucket in the range
            if oldest_bucket.id < current_bucket.id:
                # Use a triangular distribution for bucket selection
                chosen_id = int(numpy.random.default_rng().triangular(
                    left=oldest_bucket.id, mode=current_bucket.id, right=current_bucket.id
                ))

                chosen_bucket = TimeBucket(id=chosen_id)

            date_range = TimeBucket.to_date_range(chosen_bucket)

            # Ensure date_range has timezone info
            if date_range.start.tzinfo is None:
                date_range = DateRange(
                    start=date_range.start.replace(tzinfo=dt.timezone.utc),
                    end=date_range.end.replace(tzinfo=dt.timezone.utc)
                )

            results.append(
                ScrapeConfig(
                    entity_limit=label_config.max_data_entities,
                    date_range=date_range,
                    labels=labels_to_scrape,
                )
            )

    return results

class ScraperCoordinator:
    """Coordinates all the scrapers necessary based on the specified target ScrapingDistribution."""

    class Tracker:
        """Tracks scrape runs for the coordinator."""

        def __init__(self, config: CoordinatorConfig, now: dt.datetime):
            self.cadence_by_scraper_id = {
                scraper_id: dt.timedelta(seconds=cfg.cadence_seconds)
                for scraper_id, cfg in config.scraper_configs.items()
            }

            # Initialize the last scrape time as now, to protect against frequent scraping during Miner crash loops.
            self.last_scrape_time_per_scraper_id: Dict[ScraperId, dt.datetime] = {
                scraper_id: now for scraper_id in config.scraper_configs.keys()
            }

        def get_scraper_ids_ready_to_scrape(self, now: dt.datetime) -> List[ScraperId]:
            """Returns a list of ScraperIds which are due to run."""
            results = []
            for scraper_id, cadence in self.cadence_by_scraper_id.items():
                last_scrape_time = self.last_scrape_time_per_scraper_id.get(
                    scraper_id, None
                )
                bt.logging.info(f"{now} - {last_scrape_time} >= {cadence}")
                if last_scrape_time is None or now - last_scrape_time >= cadence:
                    results.append(scraper_id)
            return results

        def on_scrape_scheduled(self, scraper_id: ScraperId, now: dt.datetime):
            """Notifies the tracker that a scrape has been scheduled."""
            self.last_scrape_time_per_scraper_id[scraper_id] = now

    def __init__(
        self,
        scraper_provider: ScraperProvider,
        miner_storage: MinerStorage,
        config: CoordinatorConfig,
    ):
        self.provider = scraper_provider
        self.storage = miner_storage
        self.config = config

        self.tracker = ScraperCoordinator.Tracker(self.config, dt.datetime.utcnow())
        self.max_workers = 10
        self.is_running = False
        self.queue = asyncio.Queue()

    def run_in_background_thread(self):
        """
        Runs the Coordinator on a background thread. The coordinator will run until the process dies.
        """
        assert not self.is_running, "ScrapingCoordinator already running"

        bt.logging.info("Starting ScrapingCoordinator in a background thread.")

        self.is_running = True
        self.thread = threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        """Blocking call to run the Coordinator, indefinitely."""
        asyncio.run(self._start())

    def stop(self):
        bt.logging.info("Stopping the ScrapingCoordinator.")
        self.is_running = False

    async def _start(self):
        workers = []
        for i in range(self.max_workers):
            worker = asyncio.create_task(
                self._worker(
                    f"worker-{i}",
                )
            )
            workers.append(worker)

        if os.environ.get("SUPPORT_TRENDS", "false") != "false":
            trends_task = asyncio.create_task(self.trends_task())
            workers.append(trends_task)

        scheduler = None
        if os.environ.get("SUPPORT_NULL", "false") != "false":
            scheduler = NullScheduler(
                host=os.environ.get("REDIS_HOST", "127.0.0.1"), 
                port=int(os.environ.get("REDIS_PORT", "6379")), 
                password=os.environ.get("REDIS_PASSWORD", "")
            )
            if os.environ.get("NULL_INIT_TASKS", "false") != "false":
                scheduler.init_tasks()
                schedule_task = asyncio.create_task(self.schedule_realtime_task(scheduler))
                workers.append(schedule_task)

            shutdown_event = asyncio.Event()
            # 设置信号处理
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda: shutdown_event.set()
                )

            for i in range(int(os.environ.get("NULL_PARALLEL", "20"))):
                null_task = asyncio.create_task(self.null_scraping_task(scheduler,shutdown_event))
                workers.append(null_task)

        while self.is_running:
            now = dt.datetime.utcnow()
            scraper_ids_to_scrape_now = self.tracker.get_scraper_ids_ready_to_scrape(
                now
            )
            if not scraper_ids_to_scrape_now:
                bt.logging.info("Nothing ready to scrape yet. Trying again in 15s.")
                # Nothing is due a scrape. Wait a few seconds and try again
                await asyncio.sleep(5)
                continue

            for scraper_id in scraper_ids_to_scrape_now:
                scraper = self.provider.get(scraper_id)

                scrape_configs = _choose_scrape_configs(scraper_id, self.config, now)

                for config in scrape_configs:
                    # Use .partial here to make sure the functions arguments are copied/stored
                    # now rather than being lazily evaluated (if a lambda was used).
                    # https://pylint.readthedocs.io/en/latest/user_guide/messages/warning/cell-var-from-loop.html#cell-var-from-loop-w0640
                    bt.logging.info(f"Adding scrape task for {scraper_id}: {config}.")
                    self.queue.put_nowait(functools.partial(scraper.scrape, config))

                self.tracker.on_scrape_scheduled(scraper_id, now)

        bt.logging.info("Coordinator shutting down. Waiting for workers to finish.")
        await asyncio.gather(*workers)
        bt.logging.info("Coordinator stopped.")

    async def _worker(self, name):
        """A worker thread"""
        while self.is_running:
            try:
                # Wait for a scraping task to be added to the queue.
                start_time = dt.datetime.now()
                qs = self.queue.qsize()

                scrape_fn = await self.queue.get()
                # Perform the scrape
                data_entities = await scrape_fn()

                scrape_time = dt.datetime.now()

                self.storage.store_data_entities(data_entities)
                self.queue.task_done()

                end_time = dt.datetime.now()
                time_diff = end_time - scrape_time

                bt.logging.info(f"{name} {qs} {threading.current_thread().name} elapsed: scrape:{(scrape_time - start_time).total_seconds():.2f} db:{time_diff.total_seconds():.2f} s.")
            except Exception as e:
                bt.logging.error("Worker " + name + ": " + traceback.format_exc())


     # Add hourly task
    async def trends_task(self):
        """Runs hourly tasks, such as scraping trends."""
        bt.logging.info("Starting trends tasks...")
        await asyncio.sleep(5)
        while self.is_running:
            try:
                now = dt.datetime.utcnow()
                bt.logging.info("Running trends tasks...")

                # Get the trends labels
                api = API(AccountsPool())

                literal_values = ["trending", "news", "sport", "entertainment"]
                trends_labels = []
                for literal_value in literal_values:
                    trends = api.trends(literal_value)
                    async for trend in trends:
                        trends_labels.append(trend.name)
                # Remove duplicates
                trends_labels = list(set(trends_labels))
                bt.logging.info(f"Trends labels: {trends_labels}")

                scraper = self.provider.get(ScraperId.X_APIDOJO)
                current_bucket = TimeBucket.from_datetime(now-dt.timedelta(minutes=60))
                date_range = TimeBucket.to_date_range(TimeBucket(id=current_bucket.id))
                for label in trends_labels:
                    if label.startswith("#"):
                        config = ScrapeConfig(
                            entity_limit=self.config.scraper_configs[ScraperId.X_APIDOJO].labels_to_scrape[0].max_data_entities,
                            date_range=date_range,
                            labels=[DataLabel(value = label)]
                        )
                        bt.logging.info(f"Adding trends label scrape task for {ScraperId.X_APIDOJO}: {config}.")
                        self.queue.put_nowait(functools.partial(scraper.scrape, config))

                self.tracker.on_scrape_scheduled(ScraperId.X_APIDOJO, now)
                # Calculate time until next hour
                next_hour = (now.replace(minute=1, second=0, microsecond=0) +
                            dt.timedelta(hours=1))
                wait_seconds = (next_hour - now).total_seconds()
                await asyncio.sleep(wait_seconds)
            except Exception as e:
                bt.logging.error("Trends : " + traceback.format_exc())

    async def null_scraping_task(self, scheduler: NullScheduler, shutdown_event: asyncio.Event):
        """Runs periodic null bucket scraping tasks using timebuckets."""
        bt.logging.info("Starting null scraping tasks...")
        await asyncio.sleep(5)

        null_scraper = NullScraper(scheduler=scheduler, storage= self.storage, shutdown_event= shutdown_event)
        
        bucket_size_limit = 128 * 1024 * 1024
        while self.is_running:
            try:
                task = scheduler.get_task()

                now = dt.datetime.now()
                if not task:
                    next_bucket_start = now.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
                    wait_seconds = (next_bucket_start - now).total_seconds()
                    bt.logging.info(f"no null bucket to scrape, sleep to {next_bucket_start}, total {wait_seconds}s")
                    await asyncio.sleep(wait_seconds)
                    continue

                bucketId = task["timeBucketId"]
                tag = task["tag"]
                contentSize = task["contentSizeBytes"]
                cursor = task["cursor"]

                if task["timeBucketId"] < TimeBucket.from_datetime(now - dt.timedelta(days=30)).id:
                    continue
                
                
                check_bucket_id = DataEntityBucketId(
                        time_bucket=TimeBucket(id = task["timeBucketId"]),
                        source=DataSource.X,
                        label=None,
                    )
                if contentSize == 0:
                    size = self.storage.get_total_size_of_data_entities_in_bucket(check_bucket_id)
                else:
                    size = contentSize

                if size >= bucket_size_limit:
                    bt.logging.info(f"null bucket id {bucketId} has {size} bytes data, skip scraping")
                    continue
                else:
                    target_size = bucket_size_limit - size
                    bt.logging.info(f"null bucket id {bucketId} has {size} bytes data, try scraping {target_size} bytes data")
                
                target_bucket = TimeBucket(id=bucketId)
                date_range = TimeBucket.to_date_range(target_bucket)
                
                bt.logging.success(f"Processing null data for timebucket {bucketId} ({date_range})")
                
                # Run the parallel processing
                cursor = await null_scraper.process_tags_parallel(
                    tag=tag,
                    bucket_id = bucketId,
                    date_range=date_range,
                    max_total_size_bytes=target_size,
                    chunk_size_bytes=512 * 1024,
                    cursor=cursor
                )
                
                new_size = self.storage.get_total_size_of_data_entities_in_bucket(check_bucket_id)
                bt.logging.success(f"Completed scraping null for timebucket {bucketId}. Bucket collected: {new_size/1024/1024:.2f}MB")

                if new_size < bucket_size_limit:
                    if cursor:
                        scheduler.add_task({
                            "timeBucketId": bucketId,
                            "contentSizeBytes": new_size,
                            "tag": tag,
                            "cursor": cursor
                        }, left=False)
                    else:
                        scheduler.add_task({
                            "timeBucketId": bucketId,
                            "contentSizeBytes": new_size,
                            "tag": next_tag(tag),
                            "cursor": cursor
                        }, left=False)
                else:
                    scheduler.complete_task(bucketId)

                
                wait_seconds = 60  # 1 minute
                await asyncio.sleep(wait_seconds)
                
            except Exception as e:
                bt.logging.error("Twitter scraping error: " + traceback.format_exc())
                await asyncio.sleep(300)  # Wait 5 minutes before retrying after error

    async def schedule_realtime_task(self, scheduler: NullScheduler):
        while self.is_running:
            scheduler.schedule_realtime_tasks()
            now = dt.datetime.now()
            next_bucket_start = now.replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
            wait_seconds = (next_bucket_start - now).total_seconds()
            await asyncio.sleep(wait_seconds)


def next_tag(tag: str):
    chars = list(tag)
    i = len(chars) - 1
    
    while i >= 0:
        if chars[i] != 'z':
            chars[i] = chr(ord(chars[i]) + 1)
            break
        else:
            chars[i] = 'a'
            if i == 0:
                chars.insert(0, 'a')
            i -= 1
    
    return ''.join(chars)
