## 2018、2019分时数据纵向对比
SELECT comb.*,day_comb.sum_entry2016,day_comb.sum_entry2017,day_comb.sum_entry2018,comb.entry2016/day_comb.sum_entry2016 day_percent2016,comb.entry2017/day_comb.sum_entry2017 day_percent2017,comb.entry2018/day_comb.sum_entry2018 day_percen2018 
from(
SELECT s1.date DATE16,s2.date DATE17,s3.date DATE18,s1.minute,s1.entry_quantity entry2016,s2.entry_quantity entry2017,s3.entry_quantity entry2018
FROM subway_minute_quantity s1,subway_minute_quantity s2,subway_minute_quantity s3
WHERE 
cast(substr(s1.date,5) AS decimal) = cast(SUBSTR(s2.date,5) AS decimal)+1 AND cast(SUBSTR(s2.date,5) AS decimal) = cast(SUBSTR(s3.date,5) AS decimal)+1
AND substr(s1.date,1,4) ='2016' AND  SUBSTR(s2.date,1,4)='2017' AND substr(s3.date,1,4) ='2018'
AND s1.minute=s2.minute AND s2.minute=s3.minute 
AND substr(s1.date,5,2)='10' 
ORDER BY DATE16,minute
)
comb
JOIN
(
SELECT s1.date DATE16,s2.date DATE17,s3.date DATE18,sum(s1.entry_quantity) sum_entry2016,sum(s2.entry_quantity) sum_entry2017,sum(s3.entry_quantity) sum_entry2018
FROM subway_minute_quantity s1,subway_minute_quantity s2,subway_minute_quantity s3
WHERE 
cast(substr(s1.date,5) AS decimal) = cast(SUBSTR(s2.date,5) AS decimal)+1 AND cast(SUBSTR(s2.date,5) AS decimal) = cast(SUBSTR(s3.date,5) AS decimal)+1
AND substr(s1.date,1,4) ='2016' AND  SUBSTR(s2.date,1,4)='2017' AND substr(s3.date,1,4) ='2018'
AND s1.minute=s2.minute AND s2.minute=s3.minute 
AND substr(s1.date,5,2)='10' 
group BY DATE16,DATE17,DATE18
) day_comb
ON comb.date16=day_comb.date16
ORDER BY DATE17,MINUTE

