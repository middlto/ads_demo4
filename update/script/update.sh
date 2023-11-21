#!/bin/bash
if [[ $# -eq 2 ]]
then
    event_day=$1
    event_hour=$2
else
    event_day=`date +"%Y%m%d"`
    event_hour=`date +"%H"`
fi

turing="./hadoop"
daily_dict="../../daily_task/data/meg_user_trade_new.txt"
realtime_dict="../../realtime_task/data/meg_user_trade_new.txt"
local_dict="../data/meg_user_trade_new.txt"
backup_dict="../data/backup//meg_user_trade_new.txt.${event_day}.${event_hour}"
turing_path="userpath.meg_user_trade_new.txt"
remove_date=`date -d "5 days ago" +"%Y%m%d"`

sort ${daily_dict} ${realtime_dict} | uniq > ${local_dict}
cp ${local_dict} ${backup_dict}
${turing} fs -test -e ${turing_path}
if [[ $? -eq 0 ]]
then
    ${turing} fs -rmr ${turing_path}
fi
${turing} fs -put ${local_dict} ${turing_path}

for file in `ls ../data/backup | grep "${remove_date}"`
do
    rm ../data/backup/${file}
done
