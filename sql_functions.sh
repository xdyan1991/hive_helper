
function funnel_with_request {
    local keys=$1
    local condition_str=$2
    echo "
    select
        $(gen_select_head_key ${keys} 't_c,t_j,t_r')
        t_r.all_req as all_req,
        t_r.all_req_dau as all_req_dau,
        t_j.all_imp as all_imp,
        t_j.all_imp_dau as all_imp_dau,
        t_j.all_click as all_click,
        t_j.all_click_ok as all_click_ok,
        t_c.all_conv as all_conv,
        t_c.all_payout as all_payout,
        t_r.pmt_req as pmt_req,
        t_r.pmt_req_dau as pmt_req_dau,
        t_r.pmt_hit as pmt_hit,
        t_r.pmt_hit_dau as pmt_hit_dau,
        t_j.pmt_click as pmt_click,
        t_j.pmt_click_ok as pmt_click_ok,
        t_c.pmt_conv as pmt_conv,
        t_c.pmt_payout as pmt_payout,
        t_r.sls_req as sls_req,
        t_r.sls_req_dau as sls_req_dau,
        t_j.sls_imp as sls_imp,
        t_j.sls_imp_dau as sls_imp_dau,
        t_j.sls_click as sls_click,
        t_j.sls_click_ok as sls_click_ok,
        t_c.sls_conv as sls_conv,
        t_c.sls_payout as sls_payout,
        t_r.normal_req as normal_req,
        t_r.normal_req_dau as normal_req_dau,
        t_j.real_imp as real_imp,
        t_j.real_imp_dau as real_imp_dau,
        t_j.pre_click as pre_click,
        t_j.pre_click_ok as pre_click_ok,
        t_c.pre_conv as pre_conv,
        t_c.pre_payout as pre_payout,
        t_j.nature_click as nature_click,
        t_j.nature_click_ok as nature_click_ok,
        t_c.nature_conv as nature_conv,
        t_c.nature_payout as nature_payout
    from (
    (select
        ${keys},
        count(*) as all_conv,
        sum(payout) as all_payout,
        count(
            case when nature_click_count>=1 then 1 else null end
        ) as nature_conv,
        sum(
            case when nature_click_count>=1 then payout else null end
        ) as nature_payout,
        count(
        case when req_type='normal' and nature_click_count is NULL then 1  else null end
        ) as pre_conv,
        sum(
        case when req_type='normal' and nature_click_count is NULL then payout else null end
        ) as pre_payout,
        count(
        case when pn_type = 'pt' and nature_click_count is NULL then 1 else null end
        ) as pmt_conv,
        sum(
        case when pn_type = 'pt' and nature_click_count is NULL then payout  else null end
        ) as pmt_payout,
        count(
        case when req_type='senseless' and nature_click_count is NULL then 1  else null end
        ) as sls_conv,
        sum(
        case when req_type='senseless' and nature_click_count is NULL then payout  else null end
        ) as sls_payout
    from
        ssp_log.conversion
    where
        ${condition_str}
    group by ${keys}
    ) t_c
    full outer join (
    select
        ${keys},
        count(server_id) as all_req,
        count(distinct(user_id)) as all_req_dau,
        count(
        case when pn_type = 'pt' then user_id else NULL end
        ) as pmt_req,
        count(distinct(
        case when pn_type = 'pt' then user_id else NULL end
        )) as pmt_req_dau,
        count(
        case when pn_type = 'pt' and errmsg = 'hit' then user_id else NULL end
        ) as pmt_hit,
        count(distinct(
        case when pn_type = 'pt' and errmsg = 'hit' then user_id else NULL end
        )) as pmt_hit_dau,
        count(
        case when req_type='normal' then user_id else NULL end
        ) as normal_req,
        count(distinct(
        case when req_type='normal' then user_id else NULL end
        )) as normal_req_dau,
        count(
        case when pn_type != 'pt' and req_type='senseless' then user_id else NULL end
        ) as sls_req,
        count(distinct(
        case when pn_type != 'pt' and req_type='senseless' then user_id else NULL end
        )) as sls_req_dau
    from
        ssp_log.request
    where
        ${condition_str}
    group by
        ${keys}
    )t_r
    on ($(gen_on_cond ${keys} 't_c,t_r'))
    full outer join (
    select
        ${keys},
        count(
            case when imp_recv_ts!='' then imp_recv_ts else null end
        ) as all_imp,
        count(distinct(
            case when imp_recv_ts!='' then user_id else null end
        )) as all_imp_dau,
        count(
            case when imp_recv_ts!='' and pn_type != 'pt' and req_type='normal' then imp_recv_ts else null end
        ) as real_imp,
        count(distinct(
            case when imp_recv_ts!='' and pn_type != 'pt' and req_type='normal' then user_id else null end
        )) as real_imp_dau,
        count(
            case when imp_recv_ts!='' and req_type='senseless' then imp_recv_ts else null end
        ) as sls_imp,
        count(distinct(
            case when imp_recv_ts!='' and req_type='senseless' then user_id else null end
        )) as sls_imp_dau,
        count(
            case when imp_recv_ts!='' and preclick='true' and req_type='normal' then imp_recv_ts else null end
        ) as pre_click,
        count(
            case when imp_recv_ts!='' and preclick='true' and req_type='normal' and redirect_ok=1 then imp_recv_ts else null end
        ) as pre_click_ok,
        count(
            case when pn_type = 'pt' then 1 else null end
        ) as pmt_click,
        count(
            case when pn_type = 'pt' and redirect_ok=1 then 1 else null end
        ) as pmt_click_ok,
        count(
            case when click_type=0 then 1 else null end
        ) as nature_click,
        count(
            case when click_type=0 and redirect_ok=1 then 1 else null end
        ) as nature_click_ok,
        count(
            case when req_type='senseless' then 1 else null end
        ) as sls_click,
        count(
            case when req_type='senseless' and redirect_ok=1 then 1 else null end
        ) as sls_click_ok,
        count(
            case when pn_type = 'pt' or preclick = 'true' or click_type is not NULL then 1 else null end
        ) as all_click,
        count(
        case when redirect_ok=1 then 1 else null end
        ) as all_click_ok
    from
        ssp_log.joined
    where
        ${condition_str} and fake!='1'
    group by
        ${keys}
    ) t_j
    on ($(gen_on_cond ${keys} 't_r,t_j'))
    );"
}

function funnel_without_request {
    local keys=$1
    local condition_str=$2
    echo "
    select
        $(gen_select_head_key ${keys} 't_c,t_j')
        t_j.all_imp as all_imp,
        t_j.all_click as all_click,
        t_j.all_click_ok as all_click_ok,
        t_c.all_conv as all_conv,
        t_c.all_payout as all_payout,
        t_j.pmt_click as pmt_click,
        t_j.pmt_click_ok as pmt_click_ok,
        t_c.pmt_conv as pmt_conv,
        t_c.pmt_payout as pmt_payout,
        t_j.real_imp as real_imp,
        t_j.sls_imp as sls_imp,
        t_j.sls_click as sls_click,
        t_j.sls_click_ok as sls_click_ok,
        t_c.sls_conv as sls_conv,
        t_c.sls_payout as sls_payout,
        t_j.pre_click as pre_click,
        t_j.pre_click_ok as pre_click_ok,
        t_c.pre_conv as pre_conv,
        t_c.pre_payout as pre_payout,
        t_j.nature_click as nature_click,
        t_j.nature_click_ok as nature_click_ok,
        t_c.nature_conv as nature_conv,
        t_c.nature_payout as nature_payout
    from (
    (select
        ${keys},
        count(*) as all_conv,
        sum(payout) as all_payout,
        count(
            case when nature_click_count>=1 then 1 else null end
        ) as nature_conv,
        sum(
            case when nature_click_count>=1 then payout else null end
        ) as nature_payout,
        count(
        case when req_type='normal' and nature_click_count is NULL then 1  else null end
        ) as pre_conv,
        sum(
        case when req_type='normal' and nature_click_count is NULL then payout else null end
        ) as pre_payout,
        count(
        case when pn_type = 'pt' and nature_click_count is NULL then 1 else null end
        ) as pmt_conv,
        sum(
        case when pn_type = 'pt' and nature_click_count is NULL then payout  else null end
        ) as pmt_payout,
        count(
        case when req_type='senseless' and nature_click_count is NULL then 1  else null end
        ) as sls_conv,
        sum(
        case when req_type='senseless' and nature_click_count is NULL then payout  else null end
        ) as sls_payout
    from
        ssp_log.conversion
    where
        ${condition_str}
    group by ${keys}
    ) t_c
    full outer join (
    select
        ${keys},
        count(
            case when imp_recv_ts!='' then imp_recv_ts else null end
        ) as all_imp,
        count(distinct(
            case when imp_recv_ts!='' then user_id else null end
        )) as all_imp_dau,
        count(
            case when imp_recv_ts!='' and pn_type != 'pt' and req_type='normal' then imp_recv_ts else null end
        ) as real_imp,
        count(distinct(
            case when imp_recv_ts!='' and pn_type != 'pt' and req_type='normal' then user_id else null end
        )) as real_imp_dau,
        count(
            case when imp_recv_ts!='' and req_type='senseless' then imp_recv_ts else null end
        ) as sls_imp,
        count(distinct(
            case when imp_recv_ts!='' and req_type='senseless' then user_id else null end
        )) as sls_imp_dau,
        count(
            case when imp_recv_ts!='' and preclick='true' and req_type='normal' then imp_recv_ts else null end
        ) as pre_click,
        count(
            case when imp_recv_ts!='' and preclick='true' and req_type='normal' and redirect_ok=1 then imp_recv_ts else null end
        ) as pre_click_ok,
        count(
            case when pn_type = 'pt' then 1 else null end
        ) as pmt_click,
        count(
            case when pn_type = 'pt' and redirect_ok=1 then 1 else null end
        ) as pmt_click_ok,
        count(
            case when click_type=0 then 1 else null end
        ) as nature_click,
        count(
            case when click_type=0 and redirect_ok=1 then 1 else null end
        ) as nature_click_ok,
        count(
            case when req_type='senseless' then 1 else null end
        ) as sls_click,
        count(
            case when req_type='senseless' and redirect_ok=1 then 1 else null end
        ) as sls_click_ok,
        count(
            case when pn_type = 'pt' or preclick = 'true' or click_type is not NULL then 1 else null end
        ) as all_click,
        count(
        case when redirect_ok=1 then 1 else null end
        ) as all_click_ok
    from
        ssp_log.joined
    where
        ${condition_str} and fake!='1'
    group by
        ${keys}
    ) t_j
    on ($(gen_on_cond ${keys} 't_c,t_j'))
    );"
}

function funnel_full {
    local wild_keys=$1
    local deep_keys=$2
    local all_keys=${wild_keys},${deep_keys}
    local condition_str=$3
    echo "
    select
        $(gen_select_head_key ${wild_keys} 't_c,t_j,t_r')
        $(gen_select_head_key ${deep_keys} 't_c,t_j')
        t_r.all_req as all_req,
        t_r.all_req_dau as all_req_dau,
        t_j.all_imp as all_imp,
        t_j.all_imp_dau as all_imp_dau,
        t_j.all_click as all_click,
        t_j.all_click_ok as all_click_ok,
        t_c.all_conv as all_conv,
        t_c.all_payout as all_payout,
        t_r.pmt_req as pmt_req,
        t_r.pmt_req_dau as pmt_req_dau,
        t_r.pmt_hit as pmt_hit,
        t_r.pmt_hit_dau as pmt_hit_dau,
        t_j.pmt_click as pmt_click,
        t_j.pmt_click_ok as pmt_click_ok,
        t_c.pmt_conv as pmt_conv,
        t_c.pmt_payout as pmt_payout,
        t_r.sls_req as sls_req,
        t_r.sls_req_dau as sls_req_dau,
        t_j.sls_imp as sls_imp,
        t_j.sls_imp_dau as sls_imp_dau,
        t_j.sls_click as sls_click,
        t_j.sls_click_ok as sls_click_ok,
        t_c.sls_conv as sls_conv,
        t_c.sls_payout as sls_payout,
        t_r.normal_req as normal_req,
        t_r.normal_req_dau as normal_req_dau,
        t_j.real_imp as real_imp,
        t_j.real_imp_dau as real_imp_dau,
        t_j.pre_click as pre_click,
        t_j.pre_click_ok as pre_click_ok,
        t_c.pre_conv as pre_conv,
        t_c.pre_payout as pre_payout,
        t_j.nature_click as nature_click,
        t_j.nature_click_ok as nature_click_ok,
        t_c.nature_conv as nature_conv,
        t_c.nature_payout as nature_payout
    from (
    (select
        ${all_keys},
        count(*) as all_conv,
        sum(payout) as all_payout,
        count(
            case when nature_click_count>=1 then 1 else null end
        ) as nature_conv,
        sum(
            case when nature_click_count>=1 then payout else null end
        ) as nature_payout,
        count(
        case when req_type='normal' and nature_click_count is NULL then 1  else null end
        ) as pre_conv,
        sum(
        case when req_type='normal' and nature_click_count is NULL then payout else null end
        ) as pre_payout,
        count(
        case when pn_type = 'pt' and nature_click_count is NULL then 1 else null end
        ) as pmt_conv,
        sum(
        case when pn_type = 'pt' and nature_click_count is NULL then payout  else null end
        ) as pmt_payout,
        count(
        case when req_type='senseless' and nature_click_count is NULL then 1  else null end
        ) as sls_conv,
        sum(
        case when req_type='senseless' and nature_click_count is NULL then payout  else null end
        ) as sls_payout
    from
        ssp_log.conversion
    where
        ${condition_str}
    group by ${all_keys}
    ) t_c
    full outer join
    (select
        ${all_keys},
        count(
            case when imp_recv_ts!='' then imp_recv_ts else null end
        ) as all_imp,
        count(distinct(
            case when imp_recv_ts!='' then user_id else null end
        )) as all_imp_dau,
        count(
            case when imp_recv_ts!='' and pn_type != 'pt' and req_type='normal' then imp_recv_ts else null end
        ) as real_imp,
        count(distinct(
            case when imp_recv_ts!='' and pn_type != 'pt' and req_type='normal' then user_id else null end
        )) as real_imp_dau,
        count(
            case when imp_recv_ts!='' and req_type='senseless' then imp_recv_ts else null end
        ) as sls_imp,
        count(distinct(
            case when imp_recv_ts!='' and req_type='senseless' then user_id else null end
        )) as sls_imp_dau,
        count(
            case when imp_recv_ts!='' and preclick='true' and req_type='normal' then imp_recv_ts else null end
        ) as pre_click,
        count(
            case when imp_recv_ts!='' and preclick='true' and req_type='normal' and redirect_ok=1 then imp_recv_ts else null end
        ) as pre_click_ok,
        count(
            case when pn_type = 'pt' then 1 else null end
        ) as pmt_click,
        count(
            case when pn_type = 'pt' and redirect_ok=1 then 1 else null end
        ) as pmt_click_ok,
        count(
            case when click_type=0 then 1 else null end
        ) as nature_click,
        count(
            case when click_type=0 and redirect_ok=1 then 1 else null end
        ) as nature_click_ok,
        count(
            case when req_type='senseless' then 1 else null end
        ) as sls_click,
        count(
            case when req_type='senseless' and redirect_ok=1 then 1 else null end
        ) as sls_click_ok,
        count(
            case when pn_type = 'pt' or preclick = 'true' or click_type is not NULL then 1 else null end
        ) as all_click,
        count(
        case when redirect_ok=1 then 1 else null end
        ) as all_click_ok
    from
        ssp_log.joined
    where
        ${condition_str} and fake!='1'
    group by
        ${all_keys}
    ) t_j
    on ($(gen_on_cond ${all_keys} 't_c,t_j'))
    right join
    (select
        ${wild_keys},
        count(server_id) as all_req,
        count(distinct(user_id)) as all_req_dau,
        count(
        case when pn_type = 'pt' then user_id else NULL end
        ) as pmt_req,
        count(distinct(
        case when pn_type = 'pt' then user_id else NULL end
        )) as pmt_req_dau,
        count(
        case when pn_type = 'pt' and errmsg = 'hit' then user_id else NULL end
        ) as pmt_hit,
        count(distinct(
        case when pn_type = 'pt' and errmsg = 'hit' then user_id else NULL end
        )) as pmt_hit_dau,
        count(
        case when req_type='normal' then user_id else NULL end
        ) as normal_req,
        count(distinct(
        case when req_type='normal' then user_id else NULL end
        )) as normal_req_dau,
        count(
        case when pn_type != 'pt' and req_type='senseless' then user_id else NULL end
        ) as sls_req,
        count(distinct(
        case when pn_type != 'pt' and req_type='senseless' then user_id else NULL end
        )) as sls_req_dau
    from
        ssp_log.request
    where
        ${condition_str}
    group by
        ${wild_keys}
    )t_r
    on ($(gen_on_cond ${wild_keys} 't_j,t_r'))
    );"
}

function gen_select_head_key {
    keys=$1
    tables=$2
    awk -v keys=${keys}  \
        -v tables=${tables} \
    'BEGIN{
        n_key = split(keys, key_list, ",");
        n_t = split(tables, table_list, ",");
        for (k = 1; k <= n_key; k++) {
            msg = "coalesce(";
            for (t = 1; t <= n_t; t++) {
                msg = msg""table_list[t]"."key_list[k]","
            }
            msg = msg"NULL) as "key_list[k]","
            print(msg)
        }
    }'
}


function gen_on_cond {
    keys=$1
    tables=$2
    awk -v keys=${keys} \
        -v tables=${tables} \
    'BEGIN{
        n_key = split(keys, key_list, ",");
        n_t = split(tables, table_list, ",");
        msg = "";
        for (k = 1; k <= n_key; k++) {
            if(msg)
                msg = msg" and "
            msg = msg""table_list[1]"."key_list[k]"="table_list[2]"."key_list[k];
        }
        print(msg)
    }'
}

