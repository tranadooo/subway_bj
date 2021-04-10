select line_id, station_id, start_tm, end_tm, concat_ws(',',collect_list(t1.qtty)) qtty from(
   select line_id,substr(station_id,-4) station_id,start_tm,regexp_replace(end_tm,'次日00:00','24:00') end_tm,target qtty from dos_station_period_st
   t where
   line_id not in ('82','83','84','85','93','04') and t.start_tm >= '05:00' and t.start_tm<='23:55' and start_tm not like '%次日%'
   and p_date_scope='datescope' and p_filter='exclude' and p_date_type_cd in ('2','3') and p_stat_index_type_cd='timesize'
) t1 group by t1.line_id, t1.station_id, t1.start_tm, t1.end_tm