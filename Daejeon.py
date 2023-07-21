import urllib
import pandas as pd
import requests
import json
from pandas import json_normalize
from urllib.parse import urlencode, quote_plus, unquote
import re

api_list = ['/15060591/v1/uddi:96737ba7-64e5-4a2a-91c1-fee0a12c6d7c',
            '/15060591/v1/uddi:2bb4066a-69f1-4111-a312-b7251314bd8c',
            '/15060591/v1/uddi:be1b583a-efb0-4423-9155-b46677c830e4',
            '/15060591/v1/uddi:39f5555c-26a0-4887-aa4d-e1c462515319',
            '/15060591/v1/uddi:f2963da8-3139-47eb-9926-b16f22c639a3',
            '/15060591/v1/uddi:6e5bb621-578d-4944-9ef1-dd961957a59d']

key = unquote('T8T%2FjjBK78ARKcefNxrRhtp9%2F4yOZyIy6oCQkodxwlQBiSOIpu47k79vxgk7ygVotkCqZWpNDsG7dEpjs%2BpjOw%3D%3D')

result_list = []
for idx, api in enumerate(api_list):
    print(idx, api)
    api_url = f'https://api.odcloud.kr/api{api}'
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
            type = data['구분']
            yyyymmdd = data['날짜']
            station_name = data['역명']
            station_id = data['역번호']

            for i, d in enumerate(data):
                numbers = re.findall('\d+', d)
                if d == '구분':
                    break
                start, end = int(numbers[0]), int(numbers[1])
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
daejeon_df = df[df.lane_city == '대전'].station_name.unique()