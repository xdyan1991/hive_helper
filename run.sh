#!/usr/bin/env sh   

source ./sql_functions.sh


#funnel_with_request "sv,slot" "pdate = '20170116' and pn = 'com.qihoo.security'" > ./res.sql

#funnel_with_request "sv,slot" "pdate = '20170120' and pn = 'com.qihoo.security'" | hive | sed "s:\t:,:g" | sed '/>/d' > ./table.csv

cat ./target.sql | hive | sed "s:\t:,:g" | sed '/>/d' > ./table.csv