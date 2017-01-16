#!/usr/bin/env python3
# -*- coding: utf-8 -*-


file_in = open('../ReportScript/data/all')
file_out = open('../ReportScript/data/all2.csv', '+w')

input_sep = ','
output_sep = ','
title = [
    'pkg',
    'i_not_clk',
    'i_not_clk_i',
    'i_not_clk_c',
    'i_sls_clk',
    'i_sls_clk_i',
    'i_sls_clk_c',
    'i_pre_clk',
    'i_pre_clk_i',
    'i_pre_clk_c',
    'i_ntr_clk',
    'i_ntr_clk_i',
    'i_ntr_clk_c',
    'ui_not_clk',
    'ui_not_clk_i',
    'ui_not_clk_c',
    'ui_sls_clk',
    'ui_sls_clk_i',
    'ui_sls_clk_c',
    'ui_pre_clk',
    'ui_pre_clk_i',
    'ui_pre_clk_c',
    'ui_ntr_clk',
    'ui_ntr_clk_i',
    'ui_ntr_clk_c'
]
dict = {}
for line in file_in.readlines():
    line_r = line.rstrip('\r\n')
    key = line_r.split(input_sep)[0]
    num_list = line_r.split(input_sep)[1:]
    if key not in dict:
        dict[key] = [0] * len(num_list)
    for i in range(len(num_list)):
        dict[key][i] += int(num_list[i])
if title:
    file_out.write(output_sep.join(title))

for key in dict:
    items = [str(num) for num in dict[key]]
    file_out.write(key + output_sep + output_sep.join(items) + '\r\n')

