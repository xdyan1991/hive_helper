#!/usr/bin/env sh   

source ./sql_functions.sh


rm ./res.sql
funnel_with_request "sv,slot" "pdate = '20170115' and pn = 'com.qihoo.security'" > ./res.sql

