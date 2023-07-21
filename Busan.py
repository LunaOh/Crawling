import urllib
import pandas as pd
import requests
import json
from pandas import json_normalize
from urllib.parse import urlencode, quote_plus, unquote
import openpyxl
from datetime import datetime

api_list = ['/3057229/v1/uddi:2703d6b3-909e-4662-92f7-8151776c780a',
            '/3057229/v1/uddi:dfc42d53-c635-4971-9f3f-af6d43d6be50',
            '/3057229/v1/uddi:97ba9aa7-d4c0-4ed2-8b70-ce1e41ffeece',
            '/3057229/v1/uddi:a55e8073-16b4-4db7-bb83-d81165a78ca7',
            '/3057229/v1/uddi:a788843f-d003-4c41-a4a4-84e2e7071f09',
            '/3057229/v1/uddi:97fa3e9e-02b1-4fa9-8568-bd816973fb5c',
            '/3057229/v1/uddi:af69d5cc-d0a6-4239-8321-b0bdb6e92071',
            '/3057229/v1/uddi:c71a2b58-4e61-41b2-856f-9ffe94fa8031',
            '/3057229/v1/uddi:6f4e8247-dbcf-4868-9c55-d4917c7ae8be',
            '/3057229/v1/uddi:8bbfc110-6fae-4e25-a493-e17cf595750f',
            '/3057229/v1/uddi:9a7acab6-9230-41ca-aa5f-e6ad3e90244f',
            '/3057229/v1/uddi:792bf426-e9a5-4d26-ba1d-d6ec33c915d7',
            '/3057229/v1/uddi:86b991b3-aa81-4a06-8a09-dfd66467d677',
            '/3057229/v1/uddi:6804145e-7d05-4069-897a-e0cd95fb44f8',
            '/3057229/v1/uddi:513a8d1e-529d-403d-bce6-a8b4e2d80050',
            '/3057229/v1/uddi:b43a4cae-564c-406b-908a-a5a53ff82f9e',
            '/3057229/v1/uddi:3e283bc7-9dfc-4dc0-a742-a9a406b4b7d9',
            '/3057229/v1/uddi:36fbac7d-7782-416f-a5c0-c31b7d37dba9']

key = unquote('sRY6Oxlt%2BrxzrBd1NLko7%2FaedQ3DNdsE5RbveGKUFah5t2mBZtiiWJJLHrwyQocD8sGwEKmaloLaTvyApBteqA%3D%3D')

result_list = []

for api in api_list:
    print(api)
    api_url = f'https://api.odcloud.kr/api{api}'
    cnt = 0
    total_count = 100000
    page = 1
    while page == 1 or cnt < total_count:
        #print(page)
        queryParams = '?' + urlencode({quote_plus('serviceKey'): key,
                                        quote_plus('page'): page,
                                        quote_plus('perPage'): 1500})

        url = api_url + queryParams
        text = urllib.request.urlopen(url).read().decode('utf-8')
        json_return = json.loads(text)

        if page == 1:
            total_count = json_return['totalCount']

        cnt += json_return['currentCount']
        for data in json_return['data']:
            type = data['구분']
            yyyymmdd = data['년월일']
            station_name = data['역명']
            station_id = data['역번호']

            for i, d in enumerate(data):
                if i < 24:
                    result_list.append([type, yyyymmdd, station_name, station_id, d[:2], data[d]])

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

df = pd.read_excel('/Users/dajungoh/Desktop/Dataknows/데이터/busan_1.xlsx')
busan_df = df[df.lane_city == '부산'].station_name.unique()