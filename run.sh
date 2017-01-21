#!/usr/bin/env sh   

source ./sql_functions.sh


rm ./res.sql
#funnel_with_request "sv,slot" "pdate = '20170116' and pn = 'com.qihoo.security'" > ./res.sql

funnel_with_request "sv,slot" "pdate = '20170120' and pn = 'com.qihoo.security'" | hive | sed "s:\t:,:g" > ./table.csv