import re
import asyncio
import threading
import traceback
import bittensor as bt
from twscrape import API, AccountsPool, gather
from twscrape.models import Tweet
from typing import List, Tuple, Optional
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
from scraping.apify import ActorRunner, RunConfig
from scraping.x.model import XContent
from scraping.x import utils
import datetime as dt


class ApiDojoTwitterScraper(Scraper):
    """
    Scrapes tweets using the Apidojo Twitter Scraper: https://console.apify.com/actors/61RPP7dywgiy0JPD0.
    """

    ACTOR_ID = "61RPP7dywgiy0JPD0"

    SCRAPE_TIMEOUT_SECS = 120

    BASE_RUN_INPUT = {
        "maxRequestRetries": 5
    }

    # As of 2/5/24 this actor only takes 256 MB in the default config so we can run a full batch without hitting shared actor memory limits.
    concurrent_validates_semaphore = threading.BoundedSemaphore(20)

    def __init__(self, runner: ActorRunner = ActorRunner()):
        self.runner = runner
        self.api = API(AccountsPool())

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a DataEntity by URI."""

        async def validate_entity(entity) -> ValidationResult:
            if not utils.is_valid_twitter_url(entity.uri):
                return ValidationResult(
                    is_valid=False,
                    reason="Invalid URI.",
                    content_size_bytes_validated=entity.content_size_bytes,
                )
            attempt = 0
            max_attempts = 2

            while attempt < max_attempts:
                # Increment attempt.
                attempt += 1

                # On attempt 1 we fetch the exact number of tweets. On retry we fetch more in case they are in replies.
                tweet_count = 1 if attempt == 1 else 5

                run_input = {
                    **ApiDojoTwitterScraper.BASE_RUN_INPUT,
                    "startUrls": [entity.uri],
                    "maxItems": tweet_count,
                }
                run_config = RunConfig(
                    actor_id=ApiDojoTwitterScraper.ACTOR_ID,
                    debug_info=f"Validate {entity.uri}",
                    max_data_entities=tweet_count,
                )

                # Retrieve the tweets from Apify.
                dataset: List[dict] = None
                try:
                    dataset: List[dict] = await self.runner.run(run_config, run_input)
                except (
                        Exception
                ) as e:  # Catch all exceptions here to ensure we do not exit validation early.
                    if attempt != max_attempts:
                        # Retrying.
                        continue
                    else:
                        bt.logging.error(
                            f"Failed to run actor: {traceback.format_exc()}."
                        )
                        # This is an unfortunate situation. We have no way to distinguish a genuine failure from
                        # one caused by malicious input. In my own testing I was able to make the Actor timeout by
                        # using a bad URI. As such, we have to penalize the miner here. If we didn't they could
                        # pass malicious input for chunks they don't have.
                        return ValidationResult(
                            is_valid=False,
                            reason="Failed to run Actor. This can happen if the URI is invalid, or APIfy is having an issue.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )

                # Parse the response
                tweets, is_retweets = self._best_effort_parse_dataset(dataset)

                actual_tweet = None

                for index, tweet in enumerate(tweets):
                    if utils.normalize_url(tweet.url) == utils.normalize_url(entity.uri):
                        actual_tweet = tweet
                        is_retweet = is_retweets[index]
                        break

                bt.logging.debug(actual_tweet)
                if actual_tweet is None:
                    # Only append a failed result if on final attempt.
                    if attempt == max_attempts:
                        return ValidationResult(
                            is_valid=False,
                            reason="Tweet not found or is invalid.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                else:
                    return utils.validate_tweet_content(
                        actual_tweet=actual_tweet,
                        entity=entity,
                        is_retweet=is_retweet
                    )

        if not entities:
            return []

        # Since we are using the threading.semaphore we need to use it in a context outside of asyncio.
        bt.logging.trace("Acquiring semaphore for concurrent apidojo validations.")

        with ApiDojoTwitterScraper.concurrent_validates_semaphore:
            bt.logging.trace(
                "Acquired semaphore for concurrent apidojo validations."
            )
            results = await asyncio.gather(
                *[validate_entity(entity) for entity in entities]
            )

        return results

    async def validate_hf(self, entities) -> HFValidationResult:
        """Validate the correctness of a HFEntities by URL."""

        async def validate_hf_entity(entity) -> ValidationResult:
            if not utils.is_valid_twitter_url(entity.get('url')):
                return ValidationResult(
                    is_valid=False,
                    reason="Invalid URI.",
                    content_size_bytes_validated=0,
                )

            attempt = 0
            max_attempts = 2

            while attempt < max_attempts:
                # Increment attempt.
                attempt += 1

                # On attempt 1 we fetch the exact number of tweets. On retry we fetch more in case they are in replies.
                tweet_count = 1 if attempt == 1 else 5

                run_input = {
                    **ApiDojoTwitterScraper.BASE_RUN_INPUT,
                    "startUrls": [entity.get('url')],
                    "maxItems": tweet_count,
                }
                run_config = RunConfig(
                    actor_id=ApiDojoTwitterScraper.ACTOR_ID,
                    debug_info=f"Validate {entity.get('url')}",
                    max_data_entities=tweet_count,
                )

                # Retrieve the tweets from Apify.
                dataset: List[dict] = None
                try:
                    dataset: List[dict] = await self.runner.run(run_config, run_input)
                except (
                        Exception
                ) as e:  # Catch all exceptions here to ensure we do not exit validation early.
                    if attempt != max_attempts:
                        # Retrying.
                        continue
                    else:
                        bt.logging.error(
                            f"Failed to run actor: {traceback.format_exc()}."
                        )
                        # This is an unfortunate situation. We have no way to distinguish a genuine failure from
                        # one caused by malicious input. In my own testing I was able to make the Actor timeout by
                        # using a bad URI. As such, we have to penalize the miner here. If we didn't they could
                        # pass malicious input for chunks they don't have.
                        return ValidationResult(
                            is_valid=False,
                            reason="Failed to run Actor. This can happen if the URI is invalid, or APIfy is having an issue.",
                            content_size_bytes_validated=0,
                        )

                # Parse the response
                tweets = self._best_effort_parse_hf_dataset(dataset)
                actual_tweet = None

                for index, tweet in enumerate(tweets):
                    if utils.normalize_url(tweet['url']) == utils.normalize_url(entity.get('url')):
                        actual_tweet = tweet
                        break

                bt.logging.debug(actual_tweet)
                if actual_tweet is None:
                    # Only append a failed result if on final attempt.
                    if attempt == max_attempts:
                        return ValidationResult(
                            is_valid=False,
                            reason="Tweet not found or is invalid.",
                            content_size_bytes_validated=0,
                        )
                else:
                    return utils.validate_hf_retrieved_tweet(
                        actual_tweet=actual_tweet,
                        tweet_to_verify=entity
                    )

        # Since we are using the threading.semaphore we need to use it in a context outside of asyncio.
        bt.logging.trace("Acquiring semaphore for concurrent apidojo validations.")

        with ApiDojoTwitterScraper.concurrent_validates_semaphore:
            bt.logging.trace(
                "Acquired semaphore for concurrent apidojo validations."
            )
            results = await asyncio.gather(
                *[validate_hf_entity(entity) for entity in entities]
            )

        is_valid, valid_percent = utils.hf_tweet_validation(validation_results=results)
        return HFValidationResult(is_valid=is_valid, validation_percentage=valid_percent,
                                  reason=f"Validation Percentage = {valid_percent}")

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""
        # Construct the query string.
        date_format = "%Y-%m-%d_%H:%M:%S_UTC"

        query_parts = []

        # Add date range
        query_parts.append(
            f"since:{scrape_config.date_range.start.astimezone(tz=dt.timezone.utc).strftime(date_format)}")
        query_parts.append(f"until:{scrape_config.date_range.end.astimezone(tz=dt.timezone.utc).strftime(date_format)}")

        # Handle labels - separate usernames and keywords
        if scrape_config.labels:
            username_labels = []
            keyword_labels = []

            for label in scrape_config.labels:
                if label.value.startswith('@'):
                    # Remove @ for the API query
                    username = label.value[1:]
                    username_labels.append(f"from:{username}")
                else:
                    keyword_labels.append(label.value)

            # Add usernames with OR between them
            if username_labels:
                query_parts.append(f"({' OR '.join(username_labels)})")

            # Add keywords with OR between them if there are any
            if keyword_labels:
                query_parts.append(f"({' OR '.join(keyword_labels)})")
        else:
            # HACK: The search query doesn't work if only a time range is provided.
            # If no label is specified, just search for "e", the most common letter in the English alphabet.
            query_parts.append("e")

        # Join all parts with spaces
        query = " ".join(query_parts)

        # Construct the input to the runner.
        max_items = scrape_config.entity_limit or 150
        # run_input = {
        #     **ApiDojoTwitterScraper.BASE_RUN_INPUT,
        #     "searchTerms": [query],
        #     "maxTweets": max_items,
        # }

        # run_config = RunConfig(
        #     actor_id=ApiDojoTwitterScraper.ACTOR_ID,
        #     debug_info=f"Scrape {query}",
        #     max_data_entities=scrape_config.entity_limit,
        #     timeout_secs=ApiDojoTwitterScraper.SCRAPE_TIMEOUT_SECS,
        # )

        bt.logging.success(f"Performing Twitter scrape for search terms: {query}.")
        start_time = dt.datetime.now()


        # Run the Actor and retrieve the scraped data.
        # dataset: List[dict] = None
        try:
            tweets = await gather(self.api.search(query,max_items))
        except Exception:
            bt.logging.error(
                f"Failed to scrape tweets using search terms {query}: {traceback.format_exc()}."
            )
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        x_contents, is_retweets, skip_count = self._best_effort_parse_tweets(tweets)

        end_time = dt.datetime.now()
        time_diff = end_time - start_time
        bt.logging.success(
            f"Completed scrape for {query}. Scraped {len(x_contents)} items, skip {skip_count} old age tweets. elapsed time: {time_diff.total_seconds():.2f} seconds."
        )

        data_entities = []
        for x_content in x_contents:
            data_entities.append(XContent.to_data_entity(content=x_content))

        return data_entities

    def _best_effort_parse_tweets(self, tweets: list[Tweet]) -> Tuple[List[XContent], List[bool], int]:
        """Performs a best effort parsing of Apify dataset into List[XContent]

        Any errors are logged and ignored."""
        
        skip_count = 0
        if len(tweets) == 0:  # Todo remove first statement if it's not necessary
            return [], [], skip_count

        results: List[XContent] = []
        is_retweets: List[bool] = []

        age_limit = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
        for tweet in tweets:
            try:
                now = dt.datetime.now(dt.timezone.utc)
                if tweet.date < age_limit:
                    skip_count+=1
                    # Skip tweets that are older than the data entity bucket age limit.
                    continue

                # Check that we have the required fields.
                # if (
                #         ("text" not in data)
                #         or "url" not in data
                #         or "createdAt" not in data
                # ):
                #     continue

                text = tweet.rawContent #data['text']

                # Apidojo returns cashtags separately under symbols.
                # These are returned as list of dicts where the indices key is the first/last index and text is the tag.
                # If there are no hashtags or cashtags they are empty lists.

                # Safely retrieve hashtags and symbols lists using dictionary.get() method
                # hashtags = data.get('hashtags', [])
                # cashtags = data.get('symbols', [])
      
                hashtags = []
                cashtags = []
                
                
                def find_hashtag_position(text, hashtag):
                    """
                    查找文本中 hashtag 的起始和结束位置
                    
                    Args:
                        text (str): 要搜索的文本
                        hashtag (str): 要查找的 hashtag（包括 #）
                    
                    Returns:
                        tuple: (start, end) 位置，如果没有找到则返回 None
                    """
                    start = text.find('#'+hashtag)
                    if start == -1:
                        return None
                    end = start + len(hashtag) + 1
                    return (start, end)

                # Compile regex patterns once for better performance
                # hashtag_pattern = re.compile(
                #     r'(#)(?!\d+\b)(['
                #     r'\w'  # 字母、数字、下划线（A-Za-z0-9_）
                #     r'\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF'  # 阿拉伯文
                #     r'\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF'  # 中日韩（CJK）
                #     r'\u0E00-\u0E7F'  # 泰文
                #     r'\uAC00-\uD7AF\u1100-\u11FF'  # 韩文
                #     r'\u0900-\u097F\u0980-\u09FF'  # 梵文、孟加拉文等
                #     r'\u200C'  # Zero-width non-joiner (ZWNJ)
                #     r']+)',
                #     re.UNICODE
                # )
                # hashtag_pattern = re.compile(r'(#)([\w\u0600-\u06FF\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\u200C]+)')  # Matches #prefix with Unicode
                cashtag_pattern = re.compile(r'(\$)([A-Za-z]{1,5}\b)')      # Matches $prefix with 1-5 letters
                
                # Find all hashtag matches
                # for match in hashtag_pattern.finditer(text):
                #     hashtags.append({
                #         "text": match.group(2),   # Second group (actual content)
                #         "indices": [match.start(), match.end()],  # Start and end of match}
                #     })
                
                for h in tweet.hashtags:
                    pos = find_hashtag_position(text, h)
                    if pos:
                        hashtags.append({
                            "text": h, 
                            "indices": pos
                        })
                
                # Find all symbol matches
                for match in cashtag_pattern.finditer(text):
                    cashtags.append({
                        "text": match.group(2),   # Second group (actual content)
                        "indices": [match.start(), match.end()],  # Start and end of match}
                    })

                # Combine hashtags and cashtags into one list and sort them by their first index
                sorted_tags = sorted(hashtags + cashtags, key=lambda x: x['indices'][0])          

                # Create a list of formatted tags with prefixes
                tags = ["#" + item['text'] for item in sorted_tags]

                unique_tags = []
                [unique_tags.append(x) for x in tags if x not in unique_tags]
                
                # Extract media URLs from the data
                media_urls = []
                for media_item in tweet.media.photos:
                    media_urls.append(media_item.url)
                for media_item in tweet.media.videos:
                    media_urls.append(media_item.thumbnailUrl)
                for media_item in tweet.media.animated:
                    media_urls.append(media_item.thumbnailUrl)
                # if 'media' in data and isinstance(data['media'], list):
                #     for media_item in data['media']:
                #         if isinstance(media_item, dict) and 'media_url_https' in media_item:
                #             media_urls.append(media_item['media_url_https'])
                #         elif isinstance(media_item, str):
                #             media_urls.append(media_item)
                            
                # is_retweet = data.get('isRetweet', False)
                is_retweet = tweet.retweetedTweet is not None
                is_retweets.append(is_retweet)
                results.append(
                    XContent(
                        username=tweet.user.username, #data['userName'],  # utils.extract_user(data["url"]),
                        text=utils.sanitize_scraped_tweet(text),
                        url= tweet.url, #data["url"],
                        timestamp= tweet.date.replace(tzinfo=dt.timezone.utc), #dt.datetime.strptime(
                        #     data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                        # ),
                        tweet_hashtags=unique_tags,
                        media=media_urls if media_urls else None,
                        # Enhanced fields
                        user_id=tweet.user.id_str, #,user_info['id'],
                        user_display_name=tweet.user.displayname,#user_info['display_name'],
                        user_verified=tweet.user.verified,#user_info['verified'],
                        # Non-dynamic tweet metadata
                        tweet_id=tweet.id_str,#data.get('id'),
                        is_reply=tweet.inReplyToTweetId is not None,#data.get('isReply', None),
                        is_quote=tweet.quotedTweet is not None,#data.get('isQuote', None),
                        # Additional metadata
                        conversation_id=tweet.conversationIdStr,#data.get('conversationId'),
                        in_reply_to_user_id=tweet.inReplyToTweetIdStr,#reply_info[0],
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )
        return results, is_retweets, skip_count
    
    def _best_effort_parse_dataset(self, dataset: List[dict]) -> Tuple[List[XContent], List[bool]]:
        """Performs a best effort parsing of Apify dataset into List[XContent]
        Any errors are logged and ignored."""

        if dataset == [{"zero_result": True}] or not dataset:
            return [], []

        results: List[XContent] = []
        is_retweets: List[bool] = []
        
        for data in dataset:
            try:
                # Check that we have the required fields.
                if not all(field in data for field in ["text", "url", "createdAt"]):
                    continue

                # Extract reply information (tuple of (user_id, username))
                reply_info = self._extract_reply_info(data)
                
                # Extract user information
                user_info = self._extract_user_info(data)
                
                # Extract hashtags and media
                tags = self._extract_tags(data)
                media_urls = self._extract_media_urls(data)

                is_retweet = data.get('isRetweet', False)
                is_retweets.append(is_retweet)
                
                results.append(
                    XContent(
                        username=data['author']['userName'],
                        text=utils.sanitize_scraped_tweet(data['text']),
                        url=data["url"],
                        timestamp=dt.datetime.strptime(
                            data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                        ),
                        tweet_hashtags=tags,
                        media=media_urls if media_urls else None,
                        # Enhanced fields
                        user_id=user_info['id'],
                        user_display_name=user_info['display_name'],
                        user_verified=user_info['verified'],
                        # Non-dynamic tweet metadata
                        tweet_id=data.get('id'),
                        is_reply=data.get('isReply', None),
                        is_quote=data.get('isQuote', None),
                        # Additional metadata
                        conversation_id=data.get('conversationId'),
                        in_reply_to_user_id=reply_info[0],
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )
        
        return results, is_retweets

    def _extract_reply_info(self, data: dict) -> Tuple[Optional[str], Optional[str]]:
        """Extract reply information, returning (user_id, username) or (None, None)"""
        if not data.get('isReply', False):
            return None, None
        
        user_id = data.get('inReplyToUserId')
        username = None
        
        if 'inReplyToUser' in data and isinstance(data['inReplyToUser'], dict):
            username = data['inReplyToUser'].get('userName')
        
        return user_id, username

    def _extract_user_info(self, data: dict) -> dict:
        """Extract user information from tweet"""
        if 'author' not in data or not isinstance(data['author'], dict):
            return {'id': None, 'display_name': None, 'verified': False}
        
        author = data['author']
        return {
            'id': author.get('id'),
            'display_name': author.get('name'),
            'verified': any([
                author.get('isVerified', False),
                author.get('isBlueVerified', False),
                author.get('verified', False)
            ])
        }

    def _extract_tags(self, data: dict) -> List[str]:
        """Extract and format hashtags and cashtags from tweet"""
        entities = data.get('entities', {})
        hashtags = entities.get('hashtags', [])
        cashtags = entities.get('symbols', [])
        
        # Combine and sort by index
        all_tags = sorted(hashtags + cashtags, key=lambda x: x['indices'][0])
        
        return ["#" + item['text'] for item in all_tags]

    def _extract_media_urls(self, data: dict) -> List[str]:
        """Extract media URLs from tweet"""
        media_urls = []
        media_data = data.get('media', [])
        
        if not isinstance(media_data, list):
            return media_urls
        
        for media_item in media_data:
            if isinstance(media_item, dict) and 'media_url_https' in media_item:
                media_urls.append(media_item['media_url_https'])
            elif isinstance(media_item, str):
                media_urls.append(media_item)
        
        return media_urls

    def _best_effort_parse_hf_dataset(self, dataset: List[dict]) -> List[dict]:
        """Performs a best effort parsing of Apify dataset into List[XContent]
        Any errors are logged and ignored."""
        if dataset == [{"zero_result": True}] or not dataset:  # Todo remove first statement if it's not necessary
            return []
        results: List[dict] = []
        i = 0
        for data in dataset:
            i = i + 1
            if (
                    ("text" not in data)
                    or "url" not in data
                    or "createdAt" not in data
            ):
                continue

            text = data['text']
            url = data['url']

            # Extract media URLs
            media_urls = []
            if 'media' in data and isinstance(data['media'], list):
                for media_item in data['media']:
                    if isinstance(media_item, dict) and 'media_url_https' in media_item:
                        media_urls.append(media_item['media_url_https'])
                    elif isinstance(media_item, str):
                        media_urls.append(media_item)

            results.append({
                "text": utils.sanitize_scraped_tweet(text),
                "url": url,
                "datetime": dt.datetime.strptime(
                    data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                ),
                "media": media_urls if media_urls else None
            })

        return results


async def test_scrape():
    scraper = ApiDojoTwitterScraper()

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=100,
            date_range=DateRange(
                start=dt.datetime(2024, 5, 27, 0, 0, 0, tzinfo=dt.timezone.utc),
                end=dt.datetime(2024, 5, 27, 9, 0, 0, tzinfo=dt.timezone.utc),
            ),
            labels=[DataLabel(value="#bittgergnergerojngoierjgensor")],
        )
    )

    return entities


async def test_validate():
    scraper = ApiDojoTwitterScraper()

    true_entities = [
        DataEntity(
            uri="https://x.com/0xedeon/status/1790788053960667309",
            datetime=dt.datetime(2024, 5, 15, 16, 55, 17, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#cryptocurrency"),
            content='{"username": "@0xedeon", "text": "Deux frères ont manipulé les protocoles Ethereum pour voler 25M $ selon le Département de la Justice 🕵️‍♂️💰 #Cryptocurrency #JusticeDept", "url": "https://x.com/0xedeon/status/1790788053960667309", "timestamp": "2024-05-15T16:55:00+00:00", "tweet_hashtags": ["#Cryptocurrency", "#JusticeDept"]}',
            content_size_bytes=391
        ),
        DataEntity(
            uri="https://x.com/100Xpotential/status/1790785842967101530",
            datetime=dt.datetime(2024, 5, 15, 16, 46, 30, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#catcoin"),
            content='{"username": "@100Xpotential", "text": "As i said green candles incoming 🚀🫡👇👇\\n\\nAround 15% price surge in #CatCoin 📊💸🚀🚀\\n\\n𝐂𝐨𝐦𝐦𝐞𝐧𝐭 |  𝐋𝐢𝐤𝐞 |  𝐑𝐞𝐭𝐰𝐞𝐞𝐭 |  𝐅𝐨𝐥𝐥𝐨𝐰\\n\\n#Binance #Bitcoin #PiNetwork #Blockchain #NFT #BabyDoge #Solana #PEPE #Crypto #1000x #cryptocurrency #Catcoin #100x", "url": "https://x.com/100Xpotential/status/1790785842967101530", "timestamp": "2024-05-15T16:46:00+00:00", "tweet_hashtags": ["#CatCoin", "#Binance", "#Bitcoin", "#PiNetwork", "#Blockchain", "#NFT", "#BabyDoge", "#Solana", "#PEPE", "#Crypto", "#1000x", "#cryptocurrency", "#Catcoin", "#100x"]}',
            content_size_bytes=933
        ),
        DataEntity(
            uri="https://x.com/20nineCapitaL/status/1789488160688541878",
            datetime=dt.datetime(2024, 5, 12, 2, 49, 59, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@20nineCapitaL", "text": "Yup! We agreed to. \\n\\n@MetaMaskSupport #Bitcoin #Investors #DigitalAssets #EthereumETF #Airdrops", "url": "https://x.com/20nineCapitaL/status/1789488160688541878", "timestamp": "2024-05-12T02:49:00+00:00", "tweet_hashtags": ["#Bitcoin", "#Investors", "#DigitalAssets", "#EthereumETF", "#Airdrops"]}',
            content_size_bytes=345
        ),
        DataEntity(
            uri="https://x.com/AAAlviarez/status/1790787185047658838",
            datetime=dt.datetime(2024, 5, 15, 16, 51, 50, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#web3‌‌"),
            content='{"username": "@AAAlviarez", "text": "1/3🧵\\n\\nOnce a month dozens of #web3‌‌  users show our support to one of the projects that is doing an excellent job in services and #cryptocurrency adoption.\\n\\nDo you know what Leo Power Up Day is all about?", "url": "https://x.com/AAAlviarez/status/1790787185047658838", "timestamp": "2024-05-15T16:51:00+00:00", "tweet_hashtags": ["#web3‌‌", "#cryptocurrency"]}',
            content_size_bytes=439
        ),
        DataEntity(
            uri="https://x.com/AGariaparra/status/1789488091453091936",
            datetime=dt.datetime(2024, 5, 12, 2, 49, 42, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AGariaparra", "text": "J.P Morgan, Wells Fargo hold #Bitcoin now: Why are they interested in BTC? - AMBCrypto", "url": "https://x.com/AGariaparra/status/1789488091453091936", "timestamp": "2024-05-12T02:49:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=269
        ),
        DataEntity(
            uri="https://x.com/AGariaparra/status/1789488427546939525",
            datetime=dt.datetime(2024, 5, 12, 2, 51, 2, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AGariaparra", "text": "We Asked ChatGPT if #Bitcoin Will Enter a Massive Bull Run in 2024", "url": "https://x.com/AGariaparra/status/1789488427546939525", "timestamp": "2024-05-12T02:51:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=249
        ),
        DataEntity(
            uri="https://x.com/AMikulanecs/status/1784324497895522673",
            datetime=dt.datetime(2024, 4, 27, 20, 51, 26, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#felix"),
            content='{"username": "@AMikulanecs", "text": "$FELIX The new Dog with OG Vibes... \\nWe have a clear vision for success.\\nNew Dog $FELIX \\n➡️Follow @FelixInuETH \\n➡️Join➡️Visit#memecoins #BTC #MemeCoinSeason #Bullrun2024 #Ethereum #altcoin #Crypto #meme #SOL #BaseChain #Binance", "url": "https://x.com/AMikulanecs/status/1784324497895522673", "timestamp": "2024-04-27T20:51:00+00:00", "tweet_hashtags": ["#FELIX", "#FELIX", "#memecoins", "#BTC", "#MemeCoinSeason", "#Bullrun2024", "#Ethereum", "#altcoin", "#Crypto", "#meme", "#SOL", "#BaseChain", "#Binance"]}',
            content_size_bytes=588
        ),
        DataEntity(
            uri="https://x.com/AdamEShelton/status/1789490040751411475",
            datetime=dt.datetime(2024, 5, 12, 2, 57, 27, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AdamEShelton", "text": "#bitcoin  love", "url": "https://x.com/AdamEShelton/status/1789490040751411475", "timestamp": "2024-05-12T02:57:00+00:00", "tweet_hashtags": ["#bitcoin"]}',
            content_size_bytes=199
        ),
        DataEntity(
            uri="https://x.com/AfroWestor/status/1789488798406975580",
            datetime=dt.datetime(2024, 5, 12, 2, 52, 31, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AfroWestor", "text": "Given is for Prince and princess form inheritances  to kingdom. \\n\\nWe the #BITCOIN family we Gain profits for ever. \\n\\nSo if you embrace #BTC that means you have a Kingdom to pass on for ever.", "url": "https://x.com/AfroWestor/status/1789488798406975580", "timestamp": "2024-05-12T02:52:00+00:00", "tweet_hashtags": ["#BITCOIN", "#BTC"]}',
            content_size_bytes=383
        ),
        DataEntity(
            uri="https://x.com/AlexEmidio7/status/1789488453979189327",
            datetime=dt.datetime(2024, 5, 12, 2, 51, 9, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AlexEmidio7", "text": "Bip47 V3 V4 #Bitcoin", "url": "https://x.com/AlexEmidio7/status/1789488453979189327", "timestamp": "2024-05-12T02:51:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=203
        ),
    ]
    results = await scraper.validate(entities=true_entities)
    for result in results:
        print(result)


async def test_multi_thread_validate():
    scraper = ApiDojoTwitterScraper()

    true_entities = [
        DataEntity(
            uri="https://x.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"]}',
            content_size_bytes=318,
        ),
        DataEntity(
            uri="https://x.com/HadsonNery/status/1752011223330124021",
            datetime=dt.datetime(2024, 1, 29, 16, 50, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#faleitoleve"),
            content='{"username":"@HadsonNery","text":"Se ele fosse brabo mesmo e eu estaria aqui defendendo ele, pq ele não foi direto no Davi já que a intenção dele era fazer o Davi comprar o barulho dela 🤷🏻\u200d♂️ MC fofoqueiro foi macetado pela CUNHÃ #faleitoleve","url":"https://twitter.com/HadsonNery/status/1752011223330124021","timestamp":"2024-01-29T16:50:00Z","tweet_hashtags":["#faleitoleve"]}',
            content_size_bytes=492,
        ),
    ]

    def sync_validate(entities: list[DataEntity]) -> None:
        """Synchronous version of eval_miner."""
        asyncio.run(scraper.validate(entities))

    threads = [
        threading.Thread(target=sync_validate, args=(true_entities,)) for _ in range(5)
    ]

    for thread in threads:
        thread.start()

    for t in threads:
        t.join(120)


if __name__ == "__main__":
    bt.logging.set_trace(True)
    asyncio.run(test_multi_thread_validate())
    asyncio.run(test_scrape())
    asyncio.run(test_validate())