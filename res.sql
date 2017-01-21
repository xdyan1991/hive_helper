
    select
        coalesce(t_c.sv,t_j.sv,t_r.sv,NULL) as sv,
coalesce(t_c.slot,t_j.slot,t_r.slot,NULL) as slot,
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
        sv,slot,
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
        pdate = '20170116' and pn = 'com.qihoo.security'
    group by sv,slot
    ) t_c
    full outer join (
    select
        sv,slot,
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
        pdate = '20170116' and pn = 'com.qihoo.security'
    group by
        sv,slot
    )t_r
    on (t_c.sv=t_r.sv and t_c.slot=t_r.slot)
    full outer join (
    select
        sv,slot,
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
        pdate = '20170116' and pn = 'com.qihoo.security' and fake!='1'
    group by
        sv,slot
    ) t_j
    on (t_r.sv=t_j.sv and t_r.slot=t_j.slot)
    )
