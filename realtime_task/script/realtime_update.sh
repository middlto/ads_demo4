#!/bin/bash

if [[ $# -eq 2 ]]
then
    event_day=$1
    event_hour=$2
else
    event_day=`date +"%Y%m%d"`
    event_hour=`date -d "1 hours ago" +"%H"`
fi

# get user-trade
ori_user_trade="../../user_trade/user_trade.txt"
user_trade="../data/user/user_trade.txt"
customer_trade="../data/customer/customer_trade.txt"

awk '{print $1"\t"$3"\t"$4"\t[\t]\t"$5}' ${ori_user_trade} > ${user_trade}

# get bes user
hadoop="./hadoop"
user_table_dump="afs://xxx/dump_file"
user_list="../data/user/user_list.txt"
tables=("UserTable.txt.formated")

for share in {0..3}
do
    for table in ${tables[@]}
    do
        ${hadoop} fs -test -e ${user_table_dump}/${share}/${event_day}/${event_hour}/${table}.donefile
        if [[ $? -eq 0 ]]
        then
            ${hadoop} fs -cat ${user_table_dump}/${share}/${event_day}/${event_hour}/${table} | awk -F '\x01\x01' '{print $1"\t"$7}' >> ${user_list}
        fi
    done
done

sort ${user_list} | uniq > ${user_list}.tmp
mv ${user_list}.tmp ${user_list}

# get customer-trade
awk '{if(NR==FNR){arr[$1]=$2;next;}if($1 in arr){print $1"\t"arr[$1]"\t"$3"\t"$4"\t"$5;}else{print $0;}}' ${user_list} ${ori_user_trade} > ${customer_trade}
sort -t '	' -k 5 -rn ${customer_trade} > ${customer_trade}.sort
awk '{if($2 in arr){next;}arr[$2]=1;print $2"\t"$3"\t"$4"\t[\t]\t"$5}' ${customer_trade}.sort > ${customer_trade}
rm ${customer_trade}.sort

# select user-trade
meg_user_trade="../data/user/meg_user_trade_new.txt"
customer_list="../data/customer/customer_list.txt"
awk '{if(NR==FNR){arr[$1]=$2"\t"$3"\t"$4"\t"$5"\t"$6;next;}if($1 in arr){print $1"\t"arr[$1];}}' ${user_trade} ${user_list} > ${meg_user_trade}
awk '{if(NR==FNR){arr[$1]=1;next;}if($1 in arr){next;}print;}' ${user_trade} ${user_list} > ${customer_list}

# select customer-trade
meg_customer_trade="../data/customer/meg_user_trade_new.txt"
awk '{if(NR==FNR){arr[$1]=$2"\t"$3"\t"$4"\t"$5"\t"$6;next;}if($2 in arr){print $1"\t"arr[$2];}}' ${customer_trade} ${customer_list} > ${meg_customer_trade}

# get user from idea_table
idea_table="IdeaTable.txt.formated"
idea_user_list="../data/idea_user/idea_user_list.txt"
for share in {0..3}
do
    ${hadoop} fs -test -e ${user_table_dump}/${share}/${event_day}/${event_hour}/${idea_table}.donefile
    if [[ $? -eq 0 ]]
    then
        ${hadoop} fs -cat ${user_table_dump}/${share}/${event_day}/${event_hour}/${idea_table} | awk -F '\x01\x01' '{print $6}' >> ${idea_user_list}
    fi
done
sort ${idea_user_list} | uniq > ${idea_user_list}.tmp
mv ${idea_user_list}.tmp ${idea_user_list}

# get user-trade with idea_user
meg_idea_user_trade="../data/idea_user/meg_user_trade_new.txt"
awk '{if(NR==FNR){arr[$1]=$2"\t"$3"\t"$4"\t"$5"\t"$6;next;}if($1=="NULL"){next;}if($1 in arr){print $1"\t"arr[$1];}else{print $1"\t1000\t100001\t[\t]\tempty"}}' ${user_trade} ${idea_user_list} > ${meg_idea_user_trade}

# merge user and customer trade
local_dict_path="../data/meg_user_trade_new.txt"
cat ${meg_user_trade} ${meg_customer_trade} ${meg_idea_user_trade} | sort | uniq > ${local_dict_path}
iconv -f utf8 -t gbk ${local_dict_path} > ${local_dict_path}.tmp
mv ${local_dict_path}.tmp ${local_dict_path}

# delete history
remove_date=`date -d "${event_day} 5 days ago" +"%Y%m%d"`
for file in `ls ../logs | grep "${remove_date}"`
do
    rm ../logs/${file}
done

# upload
update_path="../../update/script"
script="update.sh"
cd ${update_path} && sh -x ${script}
