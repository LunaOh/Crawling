import urllib
import pandas as pd
import requests
import json
from pandas import json_normalize
from urllib.parse import urlencode, quote_plus, unquote
import re
import datetime

api_list = [['/15002503/v1/uddi:627c4d74-f70a-4dd7-9fd0-8f7eaad7c78c', '승하', '월', '일', '역명', '역번호', 2020],
            ['/15002503/v1/uddi:36882f20-c7ac-43f1-b545-b648b5249980', '승하', '월', '일', '역명', '역번호', 2021],
            ['/15002503/v1/uddi:73edb7e2-ca66-4888-8842-d951b26655f8', '승하', '월', '일', '역명', '역번호', 2022]]

key = unquote('T8T%2FjjBK78ARKcefNxrRhtp9%2F4yOZyIy6oCQkodxwlQBiSOIpu47k79vxgk7ygVotkCqZWpNDsG7dEpjs%2BpjOw%3D%3D')

result_list = []
for idx, api in enumerate(api_list):
    print(idx, api[0])
    api_url = f'https://api.odcloud.kr/api{api[0]}'
    cnt = 0
    total_count = 100000
    page = 1
    while page == 1 or cnt < total_count:
        #print(page)
        queryParams = '?' + urlencode({quote_plus('serviceKey'): key,
                                           quote_plus('page'): page,
                                           quote_plus('perPage'): 1000})
        url = api_url + queryParams
        text = urllib.request.urlopen(url).read().decode('utf-8')
        json_return = json.loads(text)

        if page == 1:
            total_count = json_return['totalCount']

        cnt += json_return['currentCount']

        for data in json_return['data']:
            type = data[api[1]]
            year = api[6]
            mm = data[api[2]]
            dd = data[api[3]]
            yyyymmdd = datetime.date(year, int(mm), int(dd)).strftime('%Y-%m-%d')
            station_name = data[api[4]]
            station_id = data[api[5]]

            for i, d in enumerate(data):
                if d.split('-'):
                    numbers = re.findall('\d+', d)
                    if d == '승하':
                        break
                    start, end = int(numbers[0]), int(numbers[1])
                    if start == 24:
                        result_list.append([type, yyyymmdd, station_name, station_id, 0, data[d]])
                    else:
                        for time in range(start, end):
                            result_list.append([type, yyyymmdd, station_name, station_id, time, data[d]])
                if d.split('~'):
                    if len(d.split('~')) > 1:
                        start, end = int(d.split('~')[0]), int(d.split('~')[1])
                        if start == 24:
                            result_list.append([type, yyyymmdd, station_name, station_id, 0, data[d]])
                        else:
                            for time in range(start, end):
                                result_list.append([type, yyyymmdd, station_name, station_id, time, data[d]])

        page += 1

temp_df = pd.DataFrame(result_list, columns=['type', 'yyyymmdd', 'station_name', 'station_id', 'hour', 'value'])

temp_df.dropna(inplace=True)

is_ride = temp_df['type'] == '승차'
ride = temp_df[is_ride]
ride = ride.rename(columns={'value':'geton'})
ride = ride.drop(['type'], axis=1)

is_quit = temp_df['type'] == '하차'
quit = temp_df[is_quit]
quit = quit.rename(columns={'value':'getoff'})
quit = quit.drop(['type'], axis=1)

z = pd.merge(ride, quit)

z.station_name.unique()

df = pd.read_excel('C:/Users/djoh1/Downloads/odsay_station.xlsx')
daegu_df = df[df.lane_city == '대구'].station_name.unique()