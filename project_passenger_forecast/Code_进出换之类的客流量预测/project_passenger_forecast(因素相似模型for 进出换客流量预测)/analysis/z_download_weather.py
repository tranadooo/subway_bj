#coding=UTF-8
import os
import urllib.request
import re
import pandas as pd

base_dir= r"C:\Users\Administrator\Desktop\\地铁客流预测\\project_passenger_forecast"
os.chdir(base_dir)

dload_years = ["2016","2017","2018"]
weather_list = []
for year in dload_years:
    for month in range(1,13):
        time = year+str(month).zfill(2)
        url = "http://www.tianqihoubao.com/lishi/beijing/month/%s.html"%time
        headers={'User-Agent':"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3314.0 Safari/537.36 SE 2.X MetaSr"}
        request = urllib.request.Request(url, headers=headers)
        print("handle %s"%url)
        page_code = urllib.request.urlopen(request).read()
        page_code = page_code.decode('gbk')
        pattern = re.compile('天气预报">\r\n *?(.*?日).*?<td>(.*?)</td>.*?<td>(.*?)</td>.*?<td>(.*?级)</td>', re.S)
        items = re.findall(pattern, page_code)

        for i in items:
            date = i[0].strip()
            weather = i[1].replace("\r\n","").replace(" ","")
            temp = i[2].replace("\r\n","").replace(" ","")
            wind = i[3].replace("\r\n","").replace(" ","")
            weather_list.append([date,weather,temp,wind])


weather_df  = pd.DataFrame(weather_list)
weather_df.columns = ["日期","天气","温度","风力风向"]    
weather_df.to_excel("weather_data.xlsx")
 