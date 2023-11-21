#!/bin/bash

if [[ $# -eq 1 ]]
then
    event_day=$1
else
    event_day=`date -d "1 days ago" +"%Y%m%d"`
fi

turing_file="../../user_trade/user_trade.txt"
user_trade="../data/user_trade.txt"
awk '{print $1"\t"$3"\t"$4"\t[\t]\t"$5}' ${turing_file} > ${user_trade}

# get bes user
hadoop="。/hadoop"
user_table_dump="afs://xxx/dump_file"
sample_hour=("00" "04" "08" "12" "16" "20")
tables=("UserTable.txt.formated")

for share in {0..3}
do
    for table in ${tables[@]}
    do
        bes_user_list="../data/user_list/user_list_${event_day}"
        for event_hour in ${sample_hour[@]}
        do
            ${hadoop} fs -test -e ${user_table_dump}/${share}/${event_day}/${event_hour}/${table}.donefile
            if [[ $? -eq 0 ]]
            then
                ${hadoop} fs -cat ${user_table_dump}/${share}/${event_day}/${event_hour}/${table} | awk -F '\x01\x01' '{print $1}' >> ${bes_user_list}
            fi
        done
    done
done

sort ${bes_user_list} | uniq > ${bes_user_list}.tmp
mv ${bes_user_list}.tmp ${bes_user_list}

# delete history
remove_date=`date -d "${event_day} 5 days ago" +"%Y%m%d"`
for file in `ls ../data/user_list | grep "${remove_date}"`
do
    rm ../data/user_list/${file}
done

for file in `ls ../logs | grep "${remove_date}"`
do
    rm ../logs/${file}
done

# select bes user and trade
local_dict_path="../data/meg_user_trade_new.txt"
user_list_path="../data/user_list"
total_user_list="../data/user_list.txt"
total_user_trade="../data/user_trade.txt"

for file in `ls ../data/user_list`
do
    cat ../data/user_list/${file} >> ${total_user_list}.tmp
done
sort ${total_user_list}.tmp | uniq > ${total_user_list}
rm ${total_user_list}.tmp

awk '{if(NR==FNR){arr[$1]=1;next;}if($1 in arr){print;}}' ${total_user_list} ${total_user_trade} > ${local_dict_path}
iconv -f utf8 -t gbk ${local_dict_path} > ${local_dict_path}.tmp
mv ${local_dict_path}.tmp ${local_dict_path}

# upload
update_path="../../update/script"
script="update.sh"
cd ${update_path} && sh -x ${script}
