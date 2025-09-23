from scraping.reddit.model import RedditContent
from storage.miner.mysql_miner_storage import MySQLMinerStorage
from common.data import DataEntity, DataEntityBucketId, DataSource, DataLabel, TimeBucket
from datetime import datetime,timezone

tables = [
"dataentity_487152_1",
"dataentity_487632_1",
"dataentity_487656_1",
"dataentity_487680_1",
"dataentity_487704_1",
"dataentity_487728_1",
"dataentity_487752_1",
"dataentity_487776_1",
"dataentity_487800_1",
"dataentity_487824_1",
"dataentity_487848_1",
"dataentity_487872_1",
"dataentity_487896_1",
"dataentity_487920_1",
"dataentity_487944_1",
"dataentity_487968_1",
"dataentity_487992_1",
"dataentity_488016_1",
"dataentity_488040_1",
"dataentity_488064_1",
"dataentity_488088_1",
"dataentity_488112_1",
"dataentity_488136_1",
"dataentity_488160_1",
"dataentity_488184_1",
"dataentity_488208_1",
"dataentity_488232_1",
"dataentity_488256_1",
"dataentity_488280_1",
"dataentity_488304_1",
"dataentity_488328_1",
"dataentity_488352_1",
"dataentity_488376_1",
"dataentity_488400_1",
"dataentity_488424_1",
"dataentity_488448_1",
"dataentity_488472_1",
"dataentity_488496_1"
]

storage = MySQLMinerStorage()
with storage._create_connection() as connection:
    for table_name in tables:
        with connection.cursor(buffered=True) as cursor:
            print(f"Processing table: {table_name}")
            start = datetime.now()
            cursor.execute(f"""
                    select datetime, content from {table_name}
                    where json_contains_path(cast(unhex(hex(content)) as char), 'one', '$.score');
                    """
                    )
            
            data_entities = []
            for row in cursor:
                content_str = row[1].decode("utf-8")
                content = RedditContent.parse_raw(content_str)
                dt = row[0]
                if content.score is not None:
                    content = RedditContent(
                        id=content.id,
                        url=content.url,
                        username=content.username,
                        communityName=content.communityName,
                        body=content.body,
                        createdAt=content.createdAt,
                        dataType=content.dataType,
                        title=content.title,
                        parentId=content.parentId,
                        media=content.media,
                        is_nsfw=content.is_nsfw,
                        # score=post.get("score", None),
                        # upvote_ratio=post.get("upvote_ratio", None),
                        score=None,
                        upvote_ratio=None,
                        num_comments=content.num_comments
                    )
                    de = RedditContent.to_data_entity(content)
                    data_entities.append(de)
                else:
                    print(f"No score found for {content.url}")
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