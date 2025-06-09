import mysql.connector
from mysql.connector import pooling
import os
import threading
import contextlib
import datetime as dt
from collections import defaultdict
from common import constants, utils
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
    HuggingFaceMetadata,
)
from storage.miner.miner_storage import MinerStorage
from typing import Dict, List
import bittensor as bt
class UTCDateTimeConverter(mysql.connector.conversion.MySQLConverter):
    def _DATETIME_to_python(self, value, desc=None):
        dt = super()._DATETIME_to_python(value, desc)
        if isinstance(dt, dt.datetime):
            return dt.replace(tzinfo=dt.timezone.utc)
        return dt
    
class MySQLMinerStorage(MinerStorage):
    """MySQL backed MinerStorage"""

    DATA_ENTITY_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS DataEntity (
                                uri                 VARCHAR(512)   PRIMARY KEY,
                                datetime            DATETIME(6)    NOT NULL,
                                timeBucketId        INT             NOT NULL,
                                source              INT             NOT NULL,
                                label               CHAR(150)        ,
                                content             BLOB            NOT NULL,
                                contentSizeBytes    BIGINT          NOT NULL
                                )"""

    HF_METADATA_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS HFMetaData (
                                uri                 VARCHAR(512)   PRIMARY KEY,
                                source              INT             NOT NULL,
                                updatedAt           DATETIME(6)    NOT NULL,
                                encodingKey         VARCHAR(512)
                                )"""

    def __init__(
            self,
            host='localhost',
            user='taos',
            password='taos@2025',
            database='sn13',
            max_database_size_gb_hint=250
        ):
        self.database = database
        self.connection_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'charset': 'utf8mb4',
            'use_pure': True,
            'converter_class': UTCDateTimeConverter,
            "connect_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT", 10)),
            "read_timeout": int(os.environ.get("DB_READ_TIMEOUT", 60)),
            "write_timeout": int(os.environ.get("DB_WRITE_TIMEOUT", 60)),
        }

        self.pool = pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=int(os.environ.get("DB_POOL_SIZE", 10)),
            pool_reset_session=True,
            **self.connection_config,
        )

        self.database_max_content_size_bytes = utils.gb_to_bytes(
            max_database_size_gb_hint
        )

        with contextlib.closing(self._create_connection()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                # Create the DataEntity table (if it does not already exist).
                cursor.execute(MySQLMinerStorage.DATA_ENTITY_TABLE_CREATE)

                # Create the huggingface table to store HF Info
                cursor.execute(MySQLMinerStorage.HF_METADATA_TABLE_CREATE)

        # Update the HFMetaData for miners who created this table in previous versions
        self._ensure_hf_metadata_schema()
        # Lock to avoid concurrency issues on clearing space when full.
        self.clearing_space_lock = threading.Lock()

        # Lock around the refresh for the index.
        self.cached_index_refresh_lock = threading.Lock()

        # Lock around the cached get miner index.
        self.cached_index_lock = threading.Lock()
        self.cached_index_4 = None
        self.cached_index_updated = dt.datetime.min

    def _create_connection(self):
        # conn = mysql.connector.connect(**self.connection_config)

        # return conn
        return self.pool.get_connection()

    def _ensure_hf_metadata_schema(self):
        print("Ensuring HF metadata schema...")
        # with contextlib.closing(self._create_connection()) as connection:
        #     cursor = connection.cursor()

        #     # Check if the encodingKey column exists
        #     cursor.execute("SELECT * "
        #         + "FROM information_schema.columns "
        #         + "WHERE table_name = 'HFMetaData'")
        #     columns = [column[1] for column in cursor.fetchall()]

        #     if 'encodingKey' not in columns:
        #         # Add the new column
        #         cursor.execute("ALTER TABLE HFMetaData ADD COLUMN encodingKey VARCHAR(512)")
        #         bt.logging.info("Added encodingKey column to HFMetaData table")

        #     connection.commit()

    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""

        added_content_size = 0
        for data_entity in data_entities:
            added_content_size += data_entity.content_size_bytes

        # If the total size of the store is larger than our maximum configured stored content size then ecept.
        if added_content_size > self.database_max_content_size_bytes:
            raise ValueError(
                "Content size to store: "
                + str(added_content_size)
                + " exceeds configured max: "
                + str(self.database_max_content_size_bytes)
            )

        with contextlib.closing(self._create_connection()) as connection:
            # Ensure only one thread is clearing space when necessary.
            # with self.clearing_space_lock:
                # If we would exceed our maximum configured stored content size then clear space.
            with contextlib.closing(connection.cursor()) as cursor:
                # cursor.execute("SELECT SUM(contentSizeBytes) FROM DataEntity")

                # # If there are no rows we convert the None result to 0
                # result = cursor.fetchone()
                # current_content_size = result[0] if result[0] else 0

                # if (
                #     current_content_size + added_content_size
                #     > self.database_max_content_size_bytes
                # ):
                #     content_bytes_to_clear = (
                #         self.database_max_content_size_bytes // 10
                #         if self.database_max_content_size_bytes // 10
                #         > added_content_size
                #         else added_content_size
                #     )
                #     print(f"database_max_content_size_bytes: {self.database_max_content_size_bytes}")
                #     print(f"added_content_size: {added_content_size}")
                #     print(f"content_bytes_to_clear: {content_bytes_to_clear}")
                #     self.clear_content_from_oldest(content_bytes_to_clear)

            # Parse every DataEntity into an list of value lists for inserting.
                values = []

                for data_entity in data_entities:
                    label = (
                        "NULL" if (data_entity.label is None) else data_entity.label.value
                    )
                    time_bucket_id = TimeBucket.from_datetime(data_entity.datetime).id
                    values.append(
                        [
                            data_entity.uri,
                            data_entity.datetime,
                            time_bucket_id,
                            data_entity.source,
                            label,
                            data_entity.content,
                            data_entity.content_size_bytes,
                        ]
                    )

                # Insert overwriting duplicate keys (in case of updated content).
                cursor.executemany("INSERT IGNORE INTO DataEntity VALUES (%s,%s,%s,%s,%s,%s,%s)", values)

                # Commit the insert.
                connection.commit()

    def store_hf_dataset_info(self, hf_metadatas: List[HuggingFaceMetadata]):
        with contextlib.closing(self._create_connection()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                values = [
                    (
                        hf_metadata.repo_name,
                        hf_metadata.source,
                        hf_metadata.updated_at,
                        getattr(hf_metadata, 'encoding_key', None)
                    )
                    for hf_metadata in hf_metadatas
                ]

                cursor.executemany(
                    "REPLACE INTO HFMetaData (uri, source, updatedAt, encodingKey) VALUES (%s,%s,%s,%s)", values)

                connection.commit()

    def get_earliest_data_datetime(self, source):
        query = "SELECT MIN(datetime) as earliest_date FROM DataEntity WHERE source = %s"
        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute(query, (source,))
            result = cursor.fetchone()
            return result['earliest_date'] if result and result['earliest_date'] else None

    def should_upload_hf_data(self, unique_id: str) -> bool:
        sql_query = """
            SELECT FROM_UNIXTIME(AVG(UNIX_TIMESTAMP(UpdatedAt))) AS AvgUpdatedAt
            FROM (
                SELECT UpdatedAt
                FROM HFMetaData
                WHERE uri LIKE %s
                ORDER BY UpdatedAt DESC
                LIMIT 2
            ) AS subquery;
        """
        try:
            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()
                cursor.execute(sql_query, (f"%_{unique_id}",))
                result = cursor.fetchone()

                if result is None or result[0] is None:
                    return True  # No data found, should upload

                average_datetime = result[0]  # MySQL already returns datetime object
                if isinstance(average_datetime, str): 
                    average_datetime = dt.datetime.strptime(average_datetime, "%Y-%m-%d %H:%M:%S")
                average_datetime = average_datetime.replace(tzinfo=dt.timezone.utc)

                current_datetime = dt.datetime.now(dt.timezone.utc)

                # Calculate time difference for 5100 blocks (61 200 seconds (~17 hours))
                time_difference = dt.timedelta(seconds=61200)
                threshold_datetime = current_datetime - time_difference

                return threshold_datetime > average_datetime
        except Exception as e:
            bt.logging.error(f"An error occurred: {e}")
            return False

    def get_hf_metadata(self, unique_id: str) -> List[HuggingFaceMetadata]:
        sql_query = """
            SELECT uri, source, updatedAt, 
                   CASE WHEN encodingKey IS NULL THEN '' ELSE encodingKey END as encodingKey
            FROM HFMetaData
            WHERE uri LIKE %s
            ORDER BY updatedAt DESC
            LIMIT 2;
        """

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_query, (f"%_{unique_id}",))
            hf_metadatas = []

            for row in cursor:
                hf_metadata = HuggingFaceMetadata(
                    repo_name=row['uri'],
                    source=row['source'],
                    updated_at=row['updatedAt'],
                    encoding_key=row['encodingKey'] if row['encodingKey'] != '' else None
                )
                hf_metadatas.append(hf_metadata)

        return hf_metadatas

    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucketId."""
        # Get rows that match the DataEntityBucketId.
        label = (
            "NULL"
            if (data_entity_bucket_id.label is None)
            else data_entity_bucket_id.label.value
        )

        with contextlib.closing(self._create_connection()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                cursor.execute(
                    """SELECT * FROM DataEntity 
                            WHERE timeBucketId = %s AND label = %s AND source = %s""",
                    [
                        data_entity_bucket_id.time_bucket.id,
                        label,
                        data_entity_bucket_id.source,
                    ],
                )

                # Convert the rows into DataEntity objects and return them up to the configured max chuck size.
                data_entities = []

                running_size = 0

                for row in cursor:
                    # If we have already reached the max DataEntityBucket size instead return early.
                    if running_size >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES:
                        return data_entities
                    else:
                        # Construct the new DataEntity with all non null columns.
                        data_entity = DataEntity(
                            uri=row[0],
                            datetime=row[1].replace(tzinfo=dt.timezone.utc),
                            source=DataSource(row[3]),
                            content=row[5],
                            content_size_bytes=row[6],
                            label=DataLabel(value=row[4]) if row[4] != "NULL" else None
                        )
                        bt.logging.info( 
                            f"Adding data entity {data_entity} to bucket {data_entity_bucket_id}"
                        )
                        data_entities.append(data_entity)
                        running_size += row[6]

                # If we reach the end of the cursor then return all of the data entities for this DataEntityBucket.
                bt.logging.trace(
                    f"Returning {len(data_entities)} data entities for bucket {data_entity_bucket_id}"
                )
                return data_entities

    def refresh_compressed_index(self, time_delta: dt.timedelta):
        """Refreshes the compressed MinerIndex."""
        # First check if we already have a fresh enough index, if so return immediately.
        # Since the GetMinerIndex uses a 30 minute freshness period this should be the default path with the
        # Refresh thread using a 20 minute freshness period and calling this method every 21 minutes.
        with self.cached_index_lock:
            if dt.datetime.now() - self.cached_index_updated <= time_delta:
                bt.logging.trace(
                    f"Skipping updating cached index. It is already fresher than {time_delta}."
                )
                return
            else:
                bt.logging.info(
                    f"Cached index out of {time_delta} freshness period. Refreshing cached index."
                )
        
        start = dt.datetime.now()

        # Else we take the refresh lock and check again within the lock.
        # This handles cases where multiple threads are waiting on refresh at the same time.
        with self.cached_index_refresh_lock:
            with self.cached_index_lock:
                if dt.datetime.now() - self.cached_index_updated <= time_delta:
                    bt.logging.trace(
                        "After waiting on refresh lock the index was already refreshed."
                    )
                    return

            with contextlib.closing(self._create_connection()) as connection:
                with contextlib.closing(connection.cursor()) as cursor:
                    oldest_time_bucket_id = TimeBucket.from_datetime(
                        dt.datetime.now()
                        - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
                    ).id

                    # Get sum of content_size_bytes for all rows grouped by DataEntityBucket.
                    cursor.execute(
                        """SELECT SUM(contentSizeBytes) AS bucketSize, timeBucketId, source, label FROM DataEntity
                                WHERE timeBucketId >= %s
                                GROUP BY timeBucketId, label, source
                                ORDER BY bucketSize DESC
                                LIMIT %s
                                """,
                        [
                            oldest_time_bucket_id,
                            constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4,
                        ],  # Always get the max for caching and truncate to each necessary size.
                    )

                    buckets_by_source_by_label = defaultdict(dict)

                    for row in cursor:
                        # Ensure the miner does not attempt to report more than the max DataEntityBucket size.
                        size = (
                            constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                            if row[0]
                            >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                            else row[0]
                        )

                        label = row[3] if row[3] != "NULL" else None

                        bucket = buckets_by_source_by_label[DataSource(row[2])].get(
                            label, CompressedEntityBucket(label=label)
                        )
                        bucket.sizes_bytes.append(size)
                        bucket.time_bucket_ids.append(row[1])
                        buckets_by_source_by_label[DataSource(row[2])][
                            label
                        ] = bucket

                    end = dt.datetime.now()
                    bt.logging.info(
                        f"Compressed index refresh took {(end - start).total_seconds():.2f} seconds."
                    )
                    # Convert the buckets_by_source_by_label into a list of lists of CompressedEntityBucket and return
                    bt.logging.trace("Creating protocol 4 cached index.")
                    with self.cached_index_lock:
                        self.cached_index_4 = CompressedMinerIndex(
                            sources={
                                source: list(labels_to_buckets.values())
                                for source, labels_to_buckets in buckets_by_source_by_label.items()
                            }
                        )
                        self.cached_index_updated = dt.datetime.now()
                        bt.logging.success(
                            f"Created cached index of {CompressedMinerIndex.size_bytes(self.cached_index_4)} bytes "
                            + f"across {CompressedMinerIndex.bucket_count(self.cached_index_4)} buckets."
                        )

    def list_contents_in_data_entity_buckets(
        self, data_entity_bucket_ids: List[DataEntityBucketId]
    ) -> Dict[DataEntityBucketId, List[bytes]]:
        """Lists contents for each requested DataEntityBucketId.
        Args:
            data_entity_bucket_ids (List[DataEntityBucketId]): Which buckets to get contents for.
        Returns:
            Dict[DataEntityBucketId, List[bytes]]: Map of each bucket id to contained contents.
        """
        # If no bucket ids or too many bucket ids are provided return an empty dict.
        if (
            len(data_entity_bucket_ids) == 0
            or len(data_entity_bucket_ids) > constants.BULK_BUCKETS_COUNT_LIMIT
        ):
            return defaultdict(list)

        # Get rows that match the DataEntityBucketIds.
        # Use a list of alternating ids and labels to match the upcoming sql query.
        time_bucket_ids_and_labels = list()
        for bucket_id in data_entity_bucket_ids:
            time_bucket_ids_and_labels.append(bucket_id.time_bucket.id)
            # Note that only twitter has NULL label and that all twitter labels are prefixed with #.
            # Therefore we do not need to distinguish labels by source.
            label = "NULL" if (bucket_id.label is None) else bucket_id.label.value
            time_bucket_ids_and_labels.append(label)

        with contextlib.closing(self._create_connection()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                conditions = ["(timeBucketId = %s AND label = %s)"] * len(data_entity_bucket_ids)
                query = (
                    "SELECT timeBucketId, source, label, content, contentSizeBytes FROM DataEntity "
                    f"WHERE {' OR '.join(conditions)} LIMIT %s"
                )
                cursor.execute(
                    query,
                    list(time_bucket_ids_and_labels)
                    + [constants.BULK_CONTENTS_COUNT_LIMIT],
                )

                # Get the contents from each row and return them up to the configured max size.
                buckets_ids_to_contents = defaultdict(list)
                running_size = 0

                for row in cursor:
                    if running_size < constants.BULK_CONTENTS_SIZE_LIMIT_BYTES:
                        data_entity_bucket_id = DataEntityBucketId(
                            time_bucket=TimeBucket(id=row[0]),
                            source=DataSource(row[1]),
                            label=DataLabel(value=row[2]) if row[2] != "NULL" else None
                        )
                        buckets_ids_to_contents[data_entity_bucket_id].append(
                            row[3]
                        )
                        running_size += row[4]
                    else:
                        # Return early since we hit the size limit.
                        break

                return buckets_ids_to_contents

    def get_compressed_index(
        self,
        bucket_count_limit=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4,
    ) -> CompressedMinerIndex:
        """Gets the compressed MinerIndex, which is a summary of all of the DataEntities that this MinerStorage is currently serving."""

        # Force refresh index if 10 minutes beyond refersh period. Expected to be refreshed earlier by refresh loop.
        self.refresh_compressed_index(
            time_delta=(constants.MINER_CACHE_FRESHNESS + dt.timedelta(minutes=10))
        )

        with self.cached_index_lock:
            # Only protocol 4 is supported at this time.
            return self.cached_index_4

    def clear_content_from_oldest(self, content_bytes_to_clear: int):
        """Deletes entries starting from the oldest until we have cleared the specified amount of content."""

        bt.logging.debug(f"Database full. Clearing {content_bytes_to_clear} bytes.")

        with contextlib.closing(self._create_connection()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                # TODO Investigate way to select last X bytes worth of entries in a single query.
                # Get the contentSizeBytes of each row by timestamp desc.
                cursor.execute(
                    "SELECT contentSizeBytes, datetime, uri FROM DataEntity ORDER BY datetime ASC"
                )

                running_bytes = 0
                earliest_datetime_to_clear = dt.datetime.min
                sqls = []
                last_uri = ''
                # Iterate over rows until we have found bytes to clear or we reach the end and fail.
                for row in cursor:
                    running_bytes += row[0]
                    earliest_datetime_to_clear = row[1]
                    # Once we have enough content to clear then we do so.
                    print(f"running_bytes: {running_bytes}")
                    if running_bytes > content_bytes_to_clear:
                        sqls.append(f"DELETE FROM DataEntity WHERE uri = '{last_uri}' AND datetime <= '{earliest_datetime_to_clear}'")
                    else:
                        last_uri = row[2]

                for sql in sqls:
                    print(sql)
                    cursor.execute(sql)
                
                if len(sqls) > 0:
                    connection.commit()

    def list_data_entity_buckets(self) -> List[DataEntityBucket]:
        """Lists all DataEntityBuckets for all the DataEntities that this MinerStorage is currently serving."""

        with contextlib.closing(self._create_connection()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                oldest_time_bucket_id = TimeBucket.from_datetime(
                    dt.datetime.now()
                    - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
                ).id
                # Get sum of content_size_bytes for all rows grouped by DataEntityBucket.
                cursor.execute(
                    """SELECT SUM(contentSizeBytes) AS bucketSize, timeBucketId, source, label FROM DataEntity
                            WHERE timeBucketId >= %s
                            GROUP BY timeBucketId, label, source
                            ORDER BY bucketSize DESC
                            LIMIT %s
                            """,
                    [
                        oldest_time_bucket_id,
                        constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
                    ],
                )

                data_entity_buckets = []

                for row in cursor:
                    # Ensure the miner does not attempt to report more than the max DataEntityBucket size.
                    size = (
                        constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                        if row[0]
                        >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                        else row[0]
                    )

                    # Construct the new DataEntityBucket with all non null columns.
                    data_entity_bucket_id = DataEntityBucketId(
                        time_bucket=TimeBucket(id=row[1]),
                        source=DataSource(row[2]),
                        label=(
                            DataLabel(value=row[3])
                            if row[3] != "NULL"
                            else None
                        ),
                    )

                    data_entity_bucket = DataEntityBucket(
                        id=data_entity_bucket_id, size_bytes=size
                    )

                    data_entity_buckets.append(data_entity_bucket)

                # If we reach the end of the cursor then return all of the data entity buckets.
                return data_entity_buckets

    def get_total_size_of_data_entities_in_bucket(
            self, data_entity_bucket_id: DataEntityBucketId
        ) -> int:
            """Calculates the total size in bytes of all DataEntities matching the provided DataEntityBucketId."""
            # Handle NULL label case
            label = (
                "NULL"
                if (data_entity_bucket_id.label is None)
                else data_entity_bucket_id.label.value
            )

            with contextlib.closing(self._create_connection()) as connection:
                with contextlib.closing(connection.cursor()) as cursor:
                    cursor.execute(
                        """SELECT SUM(contentSizeBytes) FROM DataEntity 
                                WHERE timeBucketId = %s AND label = %s AND source = %s""",
                        [
                            data_entity_bucket_id.time_bucket.id,
                            label,
                            data_entity_bucket_id.source,
                        ],
                    )

                    # Get the sum result
                    total_size = cursor.fetchone()[0]
                    
                    # If there are no matching rows, SUM will return NULL which becomes None in Python
                    return total_size if total_size is not None else 0