#!/usr/bin/env bash

#
# 2025/06/08
# xiechengqi
# data-universe healthcheck.sh
#

main() {

local total=$(twscrape stats | grep -oP 'Total: [0-9]+' | awk '{print $NF}')
local active=$(twscrape stats | grep 'locked_SearchTimeline' | awk '{print $NF}' | sed 's/,//g')
[ ".${active}" = "." ] && local active=$(twscrape stats | grep -oP 'Active: [0-9]+' | awk '{print $NF}' | sed 's/,//g')
local activePercent=$(echo "(100 * ${active}) / ${total}" | bc)
[ "${activePercent}" -lt "20" ] && echo "Current active accounts number ${active}, total accounts number ${total}, not healthy, restarting ..." && exit 1
echo "Current active accounts number ${active}, total accounts number ${total}, healthy"
exit 0

}

main $@
