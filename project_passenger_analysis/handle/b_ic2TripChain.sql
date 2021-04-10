-- 去掉相同卡和相同下车时间的重复多余数据（保留row_num最小的那条）
TRUNCATE TABLE  sta_ic_detail_tmp;
INSERT INTO sta_ic_detail_tmp
SELECT (@rownumber :=@rownumber + 1) AS rn,
	s.*
	FROM sta_ic_detail s,
	(SELECT @rownumber := 0) r	;
	
TRUNCATE TABLE sta_ic_detail;
INSERT INTO sta_ic_detail
SELECT 
	FLOW_NUM,CARD_ID,CARD_TYPE ,TRADE_TYPE ,ORG_ID ,TRADE_TIME,TRADE_STATION_NAME,MARK_TIME ,
	MARK_STATION_NAME,LINE_ID,BUS_ID,MARK_LINE_ID,MARK_BUS_ID,DOWN_TIME ,DOWN_STATION_NAME,
	TRADE_DATE,DRIVER_FACE_ID ,POS_ID,TRIP_ID ,DIRECTION ,TRANSFER_FLAG ,TRANSFER_ID
FROM sta_ic_detail_tmp p WHERE p.ROW_NUM IN (
 SELECT MIN(t.ROW_NUM) rn FROM  sta_ic_detail_tmp t GROUP BY t.CARD_ID,t.TRADE_TIME
)
;

-- 标准化站点名称
UPDATE sta_ic_detail s  SET s.MARK_STATION_NAME = (SELECT b.name FROM B_STATION b WHERE s.MARK_STATION_NAME=b.STATION_ID);
UPDATE sta_ic_detail s  SET s.DOWN_STATION_NAME = (SELECT b.name FROM B_STATION b WHERE s.DOWN_STATION_NAME=b.STATION_ID);

-- 根据ic数据得到出行链表   sta_ic_detail ---> trip_chain_ex 
-- 性能对比 基于12.5w的ic数据 python270min<->sql7s 
TRUNCATE TABLE  trip_chain_ex;
INSERT INTO trip_chain_ex(
SELECT 
	CARD_ID,
	TRADE_DATE,
	TRANSFER_ID,
	on_station_name,
	on_time,
	case when cast(SUBSTR(t.on_time,15,2) AS unsigned int)<30 then concat(SUBSTR(t.on_time,12,2),":00") ELSE  concat(SUBSTR(t.on_time,12,2),":30") END ON_TIME_BUCKET,
	down_station_name,
	down_time,
	case when cast(SUBSTR(t.down_time,15,2) AS unsigned int)<30 then concat(SUBSTR(t.down_time,12,2),":00") ELSE  concat(SUBSTR(t.down_time,12,2),":30") END DOWN_TIME_BUCKET,
	line,
	case when LOCATE(',',transfer)!=0 then replace(replace(substr(transfer,1,char_length(transfer)-INStr(REVERSE(transfer),'-')),'-','*'),',','-') end transfer,
	transfer_num
FROM 
 (
	SELECT CARD_ID,TRADE_DATE,TRANSFER_ID,
		substring_index(on_station_name,',',1) on_station_name,
		substring_index(on_time,',',1) on_time,
		substring_index(down_station_name,',',-1) down_station_name,
		substring_index(down_time,',',-1) down_time,
		replace(line,',','-') line,		
		substr(transfer,INSTR(transfer,'-')+1) transfer,
		transfer_num
	FROM(
	SELECT CARD_ID,TRADE_DATE,TRANSFER_ID,
	GROUP_CONCAT(s.MARK_STATION_NAME ORDER BY s.MARK_TIME) on_station_name,
	GROUP_CONCAT(s.MARK_TIME ORDER BY s.MARK_TIME) on_time,
	GROUP_CONCAT(s.DOWN_STATION_NAME ORDER BY s.MARK_TIME) down_station_name,
	GROUP_CONCAT(s.DOWN_TIME ORDER BY s.MARK_TIME) down_time,
	GROUP_CONCAT(s.MARK_LINE_ID ORDER BY s.MARK_TIME) line,
	GROUP_CONCAT(CONCAT(s.MARK_STATION_NAME,"-",s.DOWN_STATION_NAME) ORDER BY s.MARK_TIME) transfer,
	COUNT(1) transfer_num
	 from sta_ic_detail s
	 GROUP BY CARD_ID,TRADE_DATE,TRANSFER_ID
	) s1	 
 ) t 
);
COMMIT;
 
