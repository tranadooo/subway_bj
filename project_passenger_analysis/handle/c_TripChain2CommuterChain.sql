-- 根据出行链数据得到通勤链表   trip_chain_ex ---> commuter_trip_chain
-- 性能对比 基于4.8w的trip_chain_ex数据 python26min<->sql2s 
TRUNCATE TABLE commuter_trip_chain;
INSERT INTO commuter_trip_chain (
SELECT CARD_ID,TRADE_DATE,
	case when substr(ON_TIME_BUCKET,1,2)<17 then substring_index(ON_STATION_NAME,',',1) ELSE '' end forward_on_station_name,
	case when substr(ON_TIME_BUCKET,1,2)<17 then substring_index(DOWN_STATION_NAME,',',1) ELSE '' end forward_down_station_name,
	case when substr(ON_TIME_BUCKET,1,2)<17 then substring_index(ON_TIME_BUCKET,',',1)  ELSE '' end forward_on_time_bucket,
	case when substr(ON_TIME_BUCKET,1,2)<17 then substring_index(LINE,',',1)  ELSE '' end forward_line,
	case when (substr(ON_TIME_BUCKET,1,2)<17 AND TRANSFER_STATION IS NOT NULL)then substring_index(TRANSFER_STATION,',',1)  ELSE '' end forward_transfer_station,
	case when (substr(ON_TIME_BUCKET,1,2)<17 AND LOCATE(',',TRANSFER_NUM)!=0) or substr(ON_TIME_BUCKET,1,2)>=17
	 then substring_index(ON_STATION_NAME,',',-1)  ELSE '' end back_on_station_name,
	case when (substr(ON_TIME_BUCKET,1,2)<17 AND LOCATE(',',TRANSFER_NUM)!=0) or substr(ON_TIME_BUCKET,1,2)>=17
	 then substring_index(DOWN_STATION_NAME,',',-1)  ELSE '' end back_down_station_name,
	case when (substr(ON_TIME_BUCKET,1,2)<17 AND LOCATE(',',TRANSFER_NUM)!=0) or substr(ON_TIME_BUCKET,1,2)>=17
	 then substring_index(ON_TIME_BUCKET,',',-1)  ELSE '' end back_on_time_bucket,
	case when (substr(ON_TIME_BUCKET,1,2)<17 AND LOCATE(',',TRANSFER_NUM)!=0) or substr(ON_TIME_BUCKET,1,2)>=17
	 then substring_index(LINE,',',-1)  ELSE '' end back_line,
	case when (substr(ON_TIME_BUCKET,1,2)<17 AND LOCATE(',',TRANSFER_NUM)!=0 AND TRANSFER_STATION IS NOT NULL) 
		or substr(ON_TIME_BUCKET,1,2 AND TRANSFER_STATION IS NOT NULL)>=17
	 then substring_index(TRANSFER_STATION,',',-1)  ELSE '' end back_transfer_station	
	FROM 
	(SELECT 
		t.CARD_ID,t.TRADE_DATE,
		GROUP_CONCAT(t.ON_STATION_NAME ORDER BY t.ON_TIME) ON_STATION_NAME,
		GROUP_CONCAT(t.ON_TIME ORDER BY t.ON_TIME) ON_TIME,
		GROUP_CONCAT(t.ON_TIME_BUCKET ORDER BY t.ON_TIME) ON_TIME_BUCKET,
		GROUP_CONCAT(t.DOWN_STATION_NAME ORDER BY t.ON_TIME) DOWN_STATION_NAME,
		GROUP_CONCAT(t.DOWN_TIME ORDER BY t.ON_TIME) DOWN_TIME,
		GROUP_CONCAT(t.DOWN_TIME_BUCKET ORDER BY t.ON_TIME) DOWN_TIME_BUCKET,
		GROUP_CONCAT(t.LINE ORDER BY t.ON_TIME) LINE,
		GROUP_CONCAT(t.TRANSFER_STATION ORDER BY t.ON_TIME) TRANSFER_STATION,
		GROUP_CONCAT(t.TRANSFER_NUM) TRANSFER_NUM		
	FROM trip_chain_ex t
	GROUP BY t.CARD_ID,t.TRADE_DATE
	) t1 
);
COMMIT;