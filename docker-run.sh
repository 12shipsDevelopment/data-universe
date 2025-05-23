#!/usr/bin/env bash

# required env
export NAME="sn13-miner-test"
export PORT="38888"

# -e TWS_PROXY="" \

docker rm -f ${NAME}
docker run -itd \
-e PORT="${PORT}" \
-e NETWORK="finney" \
-e NETUID="13" \
-e COLDKEY_MNEMONIC="" \
-e HOTKEY_MNEMONIC="" \
-e MIN_STAKE_REQUIRED="1000" \
-e DUFS_USERNAME="tao" \
-e DUFS_PASSWORD="taos@2025" \
-e TWSCRAPE_ACCOUNTS_URL="https://taos-vl.databox.live/x-twscrape/sn13/05" \
-e SCRAPING_CONFIG_FILE_URL="https://taos-vl.databox.live/scripts/sn13/data-universe/scraping_config.json" \
-e DATABASE_HOST="172.18.36.81" \
-e HUGGINGFACE_TOKEN="" \
-e S3_AUTH_URL="172.18.36.81:8000" \
-e TWITTER_NUM="5" \
-p ${PORT}:${PORT} \
--name ${NAME} taos2025/sn13-data-universe:latest /app/entrypoint.sh --offline
