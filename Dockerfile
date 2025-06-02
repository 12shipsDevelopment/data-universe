FROM taos2025/sn13-data-universe-base:latest

# Set working directory
WORKDIR /app
COPY . .

RUN chmod +x ./healthcheck.sh && chmod +x ./entrypoint.sh && pip install -e . --no-cache-dir

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# required env
ENV PORT=
ENV NETWORK=
ENV NETUID=
ENV COLDKEY_MNEMONIC=
ENV HOTKEY_MNEMONIC=
ENV MIN_STAKE_REQUIRED=
ENV DUFS_USERNAME=
ENV DUFS_PASSWORD=
ENV DATABASE_HOST=
ENV REDIS_HOST=
ENV REDIS_PORT=

# option env
ENV HUGGINGFACE_TOKEN=
ENV S3_AUTH_URL=
ENV TWITTER_NUM=50
ENV SCRAPING_CONFIG_FILE_URL=
ENV TWSCRAPE_ACCOUNTS_URL=
ENV COUNTRY=
ENV SUPPORT_NULL=true
ENV NULL_BUCKET_ID_COUNT=48

HEALTHCHECK --interval=180s --timeout=10s --start-period=600s --retries=3 CMD ./healthcheck.sh || exit 1

CMD ["./entrypoint.sh"]
