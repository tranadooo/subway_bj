select t2.line_id line_id,t2.station_id station_id,t2.data_dt `日期`,t3.week `星期`,
    case when date_id between '20170701' and '20170918'then 2
        when date_id between '20170919' and '20171026'then 3
        when date_id between '20171027' and '20171231'then 4
        when date_id between '20180101' and '20180320'then 4 
        when date_id between '20180321' and '20180509'then 1
        when date_id between '20180510' and '20180912'then 2
        when date_id between '20180913' and '20181025'then 3
        when date_id between '20181026' and '20181231'then 4
        else 4 end `季节`,t2.sum_ `总量`,t2.entry_list `分时序列`
from(
  select t1.line_id,t1.station_id,t1.data_dt,sum(entry) sum_,count(entry) num_,concat('[',regexp_replace(
        concat_ws(',',
                    sort_array(
                                collect_list(
                                            concat_ws(':',stat_index_cd,cast(entry as string))
                                            )
                                )
                    ),'\\d+\:',''),']') entry_list 
   from (
            select 
            t.line_id
            ,substr(t.station_id,-4) station_id
            ,regexp_replace(t.data_dt,'-','') data_dt
            ,t.stat_index_cd
            ,sum(swipe_card_entry_quatity)  entry
            from bmnc_pmart.t98_pasgr_period_st t 
            where data_dt between '2017-06-01' and '2018-12-31'  --2019年的数据
            and stat_index_type_cd = '0060'  --60分钟粒度
            and stat_index_cd between '00600004' and '00600022' --5点到24点
            and data_stat_std_cd = '01'            
            and data_dt = stat_dt
            and prod_id <> 'S01012'
            and line_id not in ('82','83','84','85','93','04')   
            group by
            t.line_id
            ,t.station_id
            ,t.data_dt
            ,t.stat_index_cd
            ) t1
  group by t1.line_id,t1.station_id,t1.data_dt) t2,stados_pcode.bmnc_date_prop t3
where t2.data_dt=t3.date_id 
      and t3.holiday=0  --排除节假日
      and t3.work=1     --工作
      and t3.week between '1' and '5' --普通工作日
      and num_=19

