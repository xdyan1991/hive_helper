#!/usr/bin/env python3
# -*- coding: utf-8 -*-


file_in = open('../ReportScript/all.csv')
file_out = open('../ReportScript/all2.csv', '+w')

input_sep = ','
flag = 1
for line in file_in.readlines():
    line_r = line.rstrip('\r\n')
    if flag:
        file_out.write(line_r + '\r\n')
        flag = 0
        continue
    num = int(line_r.split(input_sep)[3])

    if num > 20000:
        file_out.write(line_r + '\r\n')

