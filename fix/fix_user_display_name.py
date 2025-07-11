
from storage.miner.mysql_miner_storage import MySQLMinerStorage
from common.data import DataEntity, DataEntityBucketId, DataSource, DataLabel, TimeBucket
from datetime import datetime,timezone
from scraping.x.model import XContent

tables = [
    "dataentity_485976_2",
    "dataentity_485976_null",
    "dataentity_486000_2",
    "dataentity_486000_null",
    "dataentity_486024_2",
    "dataentity_486024_null",
    "dataentity_486048_2",
    "dataentity_486048_null",
    "dataentity_486072_2",
    "dataentity_486072_null",
    "dataentity_486096_2",
    "dataentity_486096_null",
    "dataentity_486120_2",
    "dataentity_486120_null",
    "dataentity_486144_2",
    "dataentity_486144_null",
    "dataentity_486168_2",
    "dataentity_486168_null"
]

storage = MySQLMinerStorage()
with storage._create_connection() as connection:
    for table_name in tables:
        with connection.cursor(buffered=True) as cursor:
            print(f"Processing table: {table_name}")
            start = datetime.now()
            cursor.execute(f"""
                    select datetime, content from {table_name}
                    where json_contains_path(cast(unhex(hex(content)) as char), 'one', '$.user_display_name');
                    """
                    )

            # Convert the rows into DataEntity objects and return them up to the configured max chuck size.
            data_entities = []
            for row in cursor:
                # If we have already reached the max DataEntityBucket size instead return early.
                content_str = row[1].decode("utf-8")
                content = XContent.parse_raw(content_str)
                dt = row[0]
                # Construct the new DataEntity with all non null columns.
                if content.user_display_name is not None:
                    content = XContent(
                        username=content.username,
                        text=content.text,
                        url=content.url,
                        timestamp=content.timestamp.replace(second=dt.second),
                        tweet_hashtags=content.tweet_hashtags,
                        media=content.media,
                        user_id=content.user_id,
                        user_display_name=None,
                        user_verified=content.user_verified,
                        tweet_id=content.tweet_id,
                        is_reply=content.is_reply,
                        is_quote=content.is_quote,
                        conversation_id=content.conversation_id,
                        in_reply_to_user_id=content.in_reply_to_user_id
                    )
                    data_entity = XContent.to_data_entity(content=content)
                    # print(f"{content.url} {row[6]}->{data_entity.content_size_bytes}")

                    data_entities.append(data_entity)
                else:
                    print(f"No user_display_name found for {content.url}")
            end = datetime.now()
            print(f"Fetched {len(data_entities)} DataEntities in {(end - start).total_seconds():.2f} seconds for {table_name}.")
            

            values = []
            start = datetime.now()
            for data_entity in data_entities:
                values.append(
                    [
                        data_entity.content,
                        data_entity.content_size_bytes,
                        data_entity.uri,
                    ]
                )
            batch_size = 10
            total_success = 0

            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                print(f"{batch[0][1]} {batch[0][2]}")
                cursor.executemany(f"UPDATE {table_name} SET content = %s, contentSizeBytes = %s WHERE uri = %s", batch)
                connection.commit()
                total_success += len(batch)
                print(f"成功处理 {total_success}/{len(values)} 条记录")
                break
            
            end = datetime.now()
            print(f"Updated {len(values)} DataEntities in {(end - start).total_seconds():.2f} seconds for {table_name}.")
            if table_name == "dataentity_485976_2":
                break

