#!/bin/bash

if [[ $# -eq 1 ]]
then
    event_day=$1
else
    event_day=`date -d "1 days ago" +"%Y%m%d"`
fi
turing="/home/work/mengxianggen/hadoop/turing-client/hadoop/bin/hadoop"
turing_path="userpath.bes_meg_user_trade_v2/${event_day}"
local_path="../data/user_trade_${event_day}"

# get user-trade data
retry_num=282
while [[ ${retry_num} -gt 0 ]]
do
    retry_num=`expr ${retry_num} - 1`
    ${turing} fs -test -e ${turing_path}/_SUCCESS
    if [[ $? -eq 0 ]]
    then
        if [[ -a ${local_path} ]]
        then
            rm ${local_path}
        fi
        ${turing} fs -test -e ${turing_path}/_temporary
        if [[ $? -eq 0 ]]
        then
            ${turing} fs -rmr ${turing_path}/_temporary
        fi
        ${turing} fs -getmerge ${turing_path} ${local_path}
        break
    fi
    sleep 300
done

# delete history
remove_date=`date -d "${event_day} 5 days ago" +"%Y%m%d"`
for file in `ls ../data | grep "${remove_date}"`
do
    rm ../data/${file}
done

for file in `ls ../logs | grep "${remove_date}"`
do
    rm ../logs/${file}
done

# select bes user and trade
user_trade_path="../data"
total_user_trade="../user_trade.txt"

if [[ `ls -l ${user_trade_path}/user_trade_${event_day} | awk '{print $5}'` -gt 0 ]]
then
    cp ${user_trade_path}/user_trade_${event_day} ${total_user_trade}
fi
