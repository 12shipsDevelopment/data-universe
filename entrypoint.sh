#!/usr/bin/env bash

#
# 2025/05/23
# xiechengqi
# data-universe entrypoint.sh
#

# required env
[ ".${COLDKEY_MNEMONIC}" != "." ] && [ ".${HOTKEY_MNEMONIC}" != "." ] && [ ".${PORT}" != "." ] && [ ".${NETWORK}" != "." ] && [ ".${NETUID}" != "." ] && [ ".${MIN_STAKE_REQUIRED}" != "." ] && [ ".${DUFS_USERNAME}" != "." ] && [ ".${DUFS_PASSWORD}" != "." ] && [ ".${SCRAPING_CONFIG_FILE_URL}" != "." ] && [ ".${DATABASE_HOST}" != "." ] || (echo "Less Required ENV" && exit 1)
# option env
if [ ".${TWSCRAPE_ACCOUNTS_URL}" = "." ]
then
export TWSCRAPE_ACCOUNTS_URL="https://taos-vl.databox.live/x-twscrape/sn13/"$(curl -SsL -u ${DUFS_USERNAME}:${DUFS_PASSWORD} https://taos-vl.databox.live/x-twscrape/sn13?simple | sed 's/\/$//g' | sort -R | head -1)
fi
[ ".${S3_AUTH_URL}" != "." ] && export S3_AUTH_URL_OPTION="--s3_auth_url ${S3_AUTH_URL}"
[ ".${HUGGINGFACE_TOKEN}" != "." ] && export HUGGINGFACE_TOKEN="${HUGGINGFACE_TOKEN}" && export HUGGINGFACE_TOKEN_OPTION="--huggingface true"
[ ".${TWITTER_NUM}" != "." ] && export TWITTER_NUM="${TWITTER_NUM}"

# import coldkey and hotkey, bittensor==9.0.0
echo "=> btcli w regen_coldkey --wallet.name coldkey --wallet-path $HOME/.bittensor/wallets --no-use-password --quiet --mnemonic \"{COLDKEY_MNEMONIC}\" | grep -v 'The mnemonic to the new coldkey is'"
btcli w regen_coldkey --wallet.name coldkey --wallet-path $HOME/.bittensor/wallets --no-use-password --quiet --mnemonic "${COLDKEY_MNEMONIC}" | grep -v 'The mnemonic to the new coldkey is' || (echo "Import coldkey fail, exit ..." && exit 1)
echo "=> btcli w regen_hotkey --wallet.name coldkey --wallet-path $HOME/.bittensor/wallets --wallet-hotkey default --no-use-password --quiet --mnemonic \"{HOTKEY_MNEMONIC}\" | grep -v 'The mnemonic to the new hotkey is'"
btcli w regen_hotkey --wallet.name coldkey --wallet-path $HOME/.bittensor/wallets --wallet-hotkey default --no-use-password --quiet --mnemonic "${HOTKEY_MNEMONIC}" | grep -v 'The mnemonic to the new hotkey is' || (echo "Import hotkey fail, exit ..." && exit 1)

# download scraping_config.json
echo "=> download scraping_config.json"
curl -SsL -u ${DUFS_USERNAME}:${DUFS_PASSWORD} ${SCRAPING_CONFIG_FILE_URL} -o scraping_config.json || (echo "Download ${SCRAPING_CONFIG_FILE_URL} fail, exit ..." && exit 1)

# download and import twscrape twitter accounts
mkdir -p accounts
for account in $(curl -SsL -u ${DUFS_USERNAME}:${DUFS_PASSWORD} ${TWSCRAPE_ACCOUNTS_URL}?simple 2> /dev/null | grep -E '.txt$' | sort -R | head -${TWITTER_NUM})
do
echo "=> twscrape add_accounts ${account}"
curl -SsL -u ${DUFS_USERNAME}:${DUFS_PASSWORD} ${TWSCRAPE_ACCOUNTS_URL}/${account} -o accounts/${account}
twscrape add_accounts accounts/${account} username:password:email:email_password:_:cookies
done
echo "=> twscrape stats"
twscrape stats

echo "=> python neurons/miner.py --gravity --axon.port ${PORT} --subtensor.NETWORK ${NETWORK} --netuid ${NETUID} --blacklist.min_stake_required ${MIN_STAKE_REQUIRED} --wallet.name coldkey --wallet.hotkey default --neuron.debug --logging.debug --blacklist.force_validator_permit --neuron.scraping_config_file ./scraping_config.json ${S3_AUTH_URL_OPTION} ${HUGGINGFACE_TOKEN_OPTION} $@"

python neurons/miner.py --gravity --axon.port ${PORT} --subtensor.NETWORK ${NETWORK} --netuid ${NETUID} --blacklist.min_stake_required ${MIN_STAKE_REQUIRED} --wallet.name coldkey --wallet.hotkey default --neuron.debug --logging.debug --blacklist.force_validator_permit --neuron.scraping_config_file ./scraping_config.json ${S3_AUTH_URL_OPTION} ${HUGGINGFACE_TOKEN_OPTION} $@
