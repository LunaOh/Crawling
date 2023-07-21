import urllib
import pandas as pd
import requests
import json
from pandas import json_normalize
from urllib.parse import urlencode, quote_plus, unquote
import openpyxl
from datetime import datetime

api_list = ['/15004329/v1/uddi:7d09d8ec-6737-40f0-a63c-ea76e0597d9c',
            '/15004329/v1/uddi:a7a8a109-6594-49b2-a961-df65ffd490fd']

key = unquote('T8T%2FjjBK78ARKcefNxrRhtp9%2F4yOZyIy6oCQkodxwlQBiSOIpu47k79vxgk7ygVotkCqZWpNDsG7dEpjs%2BpjOw%3D%3D')

result_list = []

for api in api_list:
    api_url = f'https://api.odcloud.kr/api{api}'
    cnt = 0
    total_count = 100000
    page = 1
    while page == 1 or cnt < total_count:
        print(page)
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
            type = data['구분']
            yyyymmdd = data['통행일자']
            station_name = data['역명']
            station_line = data['호선']

            for i, d in enumerate(data):
                # 시간 형식인 경우
                if len(d.split('~')) > 1:
                    start, end = int(d.split('~')[0]), int(d.split('~')[1])
                    # 24시인 경우는 0으로 들어가도록
                    if start == 24:
                        result_list.append([type, yyyymmdd, station_name, station_line, 0, data[d]])
                    else:
                        for time in range(start, end):
                            result_list.append([type, yyyymmdd, station_name, station_line, time, data[d]])
        page += 1

temp_df = pd.DataFrame(result_list, columns=['type', 'yyyymmdd', 'station_name', 'station_line', 'hour', 'value'])
temp_df = temp_df[temp_df.yyyymmdd != '역계']
temp_df.yyyymmdd = temp_df.yyyymmdd.apply(lambda x: datetime.strptime(x, '%Y.%m.%d').date())

is_ride = temp_df['type'] == '승차'
ride = temp_df[is_ride]
ride = ride.rename(columns={'value':'geton'})
ride = ride.drop(['type'], axis=1)

is_quit = temp_df['type'] == '하차'
quit = temp_df[is_quit]
quit = quit.rename(columns={'value':'getoff'})
quit = quit.drop(['type'], axis=1)

z = pd.merge(ride, quit)

df = pd.read_excel('C:/Users/djoh1/Downloads/odsay_station.xlsx')
incheon_df = df[df.lane_city == '수도권'].station_name.unique()