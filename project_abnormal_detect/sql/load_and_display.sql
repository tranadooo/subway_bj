SELECT
t1.train_date_type as train_date_type
,t1.train_size as train_size
,from_unixtime(unix_timestamp()+24*60*60,'yyyyMMdd') as date_id
,t3.stat_line_nme as line_name
,t4.stat_station_nme as station_name
,t2.line_id as line_id
,t2.station_id as station_id
,max(case when t1.train_target = 'exit_qtty' then -1 else t2.modified_threshold end) as entry_threshold
,max(case when t1.train_target = 'entry_qtty' then -1 else t2.modified_threshold end) as exit_threshold
,max(case when t1.train_target = 'exit_qtty' then '' else t2.outliners end) as entry_outliers
,max(case when t1.train_target = 'entry_qtty' then '' else t2.outliners end) as exit_outliers
,t2.start_tm as start_tm
,t2.end_tm as end_tm
from
(SELECT
train_size, train_date_type,train_target,train_id,train_date_scope,training_time
FROM bmnc_stados.train_his_record t
WHERE t.train_date_scope = 'datescope'
and t.train_plan = '2'
ORDER BY training_time desc
limit 16
) t1
join 
(SELECT 
train_id
,line_id
,station_id
,start_tm
,end_tm
,modified_threshold
,outliners
FROM bmnc_stados.train_result_detail
) t2
on t1.train_id = t2.train_id

left join bmnc_pcode.t99_stat_line_cd t3
on t2.line_id=t3.stat_line_id
left join bmnc_pcode.t99_stat_station_cd t4
on t2.station_id=t4.stat_station_id
GROUP BY 
t1.train_date_type
,t1.train_size
,t2.station_id
,t2.start_tm
,t2.end_tm
,t2.line_id
,t3.stat_line_nme
,t4.stat_station_nme
