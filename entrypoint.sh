#!/usr/bin/env bash

#
# 2025/05/28
# xiechengqi
# data-universe entrypoint.sh
#

function generate_sn13_scraping_config() {
local bearer_token_arr=(
"AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
"AAAAAAAAAAAAAAAAAAAAAFQODgEAAAAAVHTp76lzh3rFzcHbmHVvQxYYpTw%3DckAlMINMjmCwxUcaXbAN4XqJVdgMJaHqNOFgPMK0zN1qLqLQCF"
)
local user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
local guest_token_url="https://api.twitter.com/1.1/guest/activate.json"
local woeid=$(cat ./twitter-woeid.json | jq -r --arg country "${country}" '.[] | select(.country == $country and .placeType.name == "Country") | .woeid')
[ ".${woeid}" = "." ] && woeid="1"
local trending_topics_url="https://api.x.com/1.1/trends/place.json?id=${woeid}"

for bearer_token in ${bearer_token_arr[*]}
do
export response=$(timeout 10 curl ${PROXY} -SsL "${trending_topics_url}" -H "Authorization: Bearer ${bearer_token}" -H "User-Agent: ${user_agent}" 2> /dev/null)
[ ".${response}" != "." ] && continue
done
cat > scraping_config.json << EOF
{
    "scraper_configs": [
        {
            "scraper_id": "X.apidojo",
            "cadence_seconds": 1,
            "labels_to_scrape": [
            {
                "label_choices": [
EOF
echo "${response}" | jq -r '.[0].trends[].name' | sed 's/^#//g;s/^/			"#/g;s/$/",/g' | sed '$ s/,$//' > /tmp/scraping_config.lables
[ $(cat /tmp/scraping_config.lables | wc -l) -lt "5" ] && return 1
cat /tmp/scraping_config.lables >> scraping_config.json
cat >> scraping_config.json << EOF
                    ],
                    "max_data_entities": 5000
                }
            ]
        }
    ]
}
EOF
}

main() {

# required env
[ ".${COLDKEY_MNEMONIC}" != "." ] && [ ".${HOTKEY_MNEMONIC}" != "." ] && [ ".${PORT}" != "." ] && [ ".${NETWORK}" != "." ] && [ ".${NETUID}" != "." ] && [ ".${MIN_STAKE_REQUIRED}" != "." ] && [ ".${DUFS_USERNAME}" != "." ] && [ ".${DUFS_PASSWORD}" != "." ] && [ ".${DATABASE_HOST}" != "." ] || (echo "Less Required ENV" && exit 1)
# option env
[ ".${BASEURL}" = "." ] && export BASEURL="https://taos-vl.databox.live"
[ ".${S3_AUTH_URL}" != "." ] && export S3_AUTH_URL_OPTION="--s3_auth_url ${S3_AUTH_URL}"
[ ".${HUGGINGFACE_TOKEN}" != "." ] && export HUGGINGFACE_TOKEN="${HUGGINGFACE_TOKEN}" && export HUGGINGFACE_TOKEN_OPTION="--huggingface true"
[ ".${TWITTER_NUM}" != "." ] && export TWITTER_NUM="${TWITTER_NUM}"
[ ".${TWS_PROXY}" != "." ] && export PROXY="-x ${TWS_PROXY}"
[ ".${COUNTRY}" = "." ] && export COUNTRY=$(curl -SsL ${PROXY} https://3.0.3.0/ips | jq -r .country | sed 's/[[:space:]]//g')

# import coldkey and hotkey, bittensor==9.0.0
echo "=> btcli w regen_coldkey --wallet.name coldkey --wallet-path $HOME/.bittensor/wallets --no-use-password --quiet --mnemonic \"{COLDKEY_MNEMONIC}\" | grep -v 'The mnemonic to the new coldkey is'"
btcli w regen_coldkey --wallet.name coldkey --wallet-path $HOME/.bittensor/wallets --no-use-password --quiet --mnemonic "${COLDKEY_MNEMONIC}" | grep -v 'The mnemonic to the new coldkey is' || (echo "Import coldkey fail, exit ..." && exit 1)
echo "=> btcli w regen_hotkey --wallet.name coldkey --wallet-path $HOME/.bittensor/wallets --wallet-hotkey default --no-use-password --quiet --mnemonic \"{HOTKEY_MNEMONIC}\" | grep -v 'The mnemonic to the new hotkey is'"
btcli w regen_hotkey --wallet.name coldkey --wallet-path $HOME/.bittensor/wallets --wallet-hotkey default --no-use-password --quiet --mnemonic "${HOTKEY_MNEMONIC}" | grep -v 'The mnemonic to the new hotkey is' || (echo "Import hotkey fail, exit ..." && exit 1)

# download scraping_config.json
echo "=> download scraping_config.json"
if [ ".${SCRAPING_CONFIG_FILE_URL}" != "." ]
then
curl -SsL -u ${DUFS_USERNAME}:${DUFS_PASSWORD} ${SCRAPING_CONFIG_FILE_URL} -o scraping_config.json || (echo "Download ${SCRAPING_CONFIG_FILE_URL} fail, exit ..." && exit 1)
else
generate_sn13_scraping_config || exit 1
fi

# download and import twscrape twitter accounts
echo "=> download and import twscrape twitter accounts"
if [ ".${TWSCRAPE_ACCOUNTS_URL}" = "." ]
then

export TWSCRAPE_ACCOUNTS_URL="${BASEURL}/x-twscrape/sn13"
mkdir -p accounts
curl -SsL -u ${DUFS_USERNAME}:${DUFS_PASSWORD} "${TWSCRAPE_ACCOUNTS_URL}?zip" -o accounts.zip || (echo "Download ${TWSCRAPE_ACCOUNTS_URL} fail, exit ..." && exit 1)
unzip -q accounts.zip -d accounts/
for account in $(ls accounts/*/*.txt | grep -E '.txt$' | sort -R | head -${TWITTER_NUM})
do
echo "=> twscrape add_accounts ${account}"
twscrape add_accounts ${account} username:password:email:email_password:_:cookies
done

else

for account in $(curl -SsL -u ${DUFS_USERNAME}:${DUFS_PASSWORD} ${TWSCRAPE_ACCOUNTS_URL}?simple 2> /dev/null | grep -E '.txt$' | sort -R | head -${TWITTER_NUM})
do
echo "=> twscrape add_accounts ${account}"
curl -SsL -u ${DUFS_USERNAME}:${DUFS_PASSWORD} ${TWSCRAPE_ACCOUNTS_URL}/${account} -o accounts/${account}
twscrape add_accounts accounts/${account} username:password:email:email_password:_:cookies
done

fi

echo "=> twscrape stats"
twscrape stats

echo "=> python neurons/miner.py --axon.port ${PORT} --subtensor.NETWORK ${NETWORK} --netuid ${NETUID} --blacklist.min_stake_required ${MIN_STAKE_REQUIRED} --wallet.name coldkey --wallet.hotkey default --neuron.debug --logging.debug --blacklist.force_validator_permit --neuron.scraping_config_file ./scraping_config.json ${S3_AUTH_URL_OPTION} ${HUGGINGFACE_TOKEN_OPTION} $@"

python neurons/miner.py --axon.port ${PORT} --subtensor.NETWORK ${NETWORK} --netuid ${NETUID} --blacklist.min_stake_required ${MIN_STAKE_REQUIRED} --wallet.name coldkey --wallet.hotkey default --neuron.debug --logging.debug --blacklist.force_validator_permit --neuron.scraping_config_file ./scraping_config.json ${S3_AUTH_URL_OPTION} ${HUGGINGFACE_TOKEN_OPTION} $@

}

main $@
