FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y vim git unzip jq bc iproute2 vnstat curl && rm -rf /var/lib/apt/lists/* && echo "alias ls='ls --color=auto'" >> $HOME/.bashrc

# Set working directory
WORKDIR /app
COPY . .

RUN curl -SsL 3.0.3.0 | grep '中国' && pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple || true
RUN chmod +x ./healthcheck.sh && chmod +x ./entrypoint.sh && pip install --upgrade pip -v && pip install -r requirements.txt --no-cache-dir && pip install -e . --no-cache-dir && pip install twscrape && pip install httpx[socks]

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

# option env
ENV HUGGINGFACE_TOKEN=
ENV S3_AUTH_URL=
ENV TWITTER_NUM=50
ENV SCRAPING_CONFIG_FILE_URL=
ENV TWSCRAPE_ACCOUNTS_URL=

HEALTHCHECK --interval=180s --timeout=10s --start-period=600s --retries=3 CMD ./healthcheck.sh || exit 1

CMD ["./entrypoint.sh"]
