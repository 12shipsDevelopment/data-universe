# Use Python base image
FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y iproute2 vnstat curl && rm -rf /var/lib/apt/lists/* && echo "alias ls='ls --color=auto'" >> $HOME/.bashrc

# Set working directory
WORKDIR /app
COPY . .

RUN chmod +x ./entrypoint.sh && pip install --upgrade pip -v && pip install -r requirements.txt --no-cache-dir && pip install -e . --no-cache-dir && pip install twscrape && pip install httpx[socks]

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
ENV TWSCRAPE_ACCOUNTS_URL=
ENV SCRAPING_CONFIG_FILE_URL=
ENV DATABASE_HOST=

# option env
ENV HUGGINGFACE_TOKEN=
ENV S3_AUTH_URL=
ENV TWITTER_NUM=50

CMD ["./entrypoint.sh"]
