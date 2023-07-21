import urllib
import pandas as pd
import requests
import json
from pandas import json_normalize
from urllib.parse import urlencode, quote_plus, unquote
import openpyxl

api_list = [['/15060048/v1/uddi:95662a7a-8d7a-44c6-a1c8-c0d124aae285', '승하차구분', '날짜', '역명', '역번호'],
            ['/15060048/v1/uddi:de627fb1-53bf-48a4-b81c-ecab90119664', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:5b091edf-8d39-47e6-bbf9-eb06fdd883c0', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:1d70401b-3d1f-493a-959c-809e268009b3', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:a8f3c335-a1a6-410d-bbdf-e3487c0acdce', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:aed92778-5e52-4a11-8238-e4f1c3219615', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:cf07b35c-eb84-4849-9010-c5b3802a3365', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:0f2d41f2-9a7c-4401-b2bc-f7468e37e3d2', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:16c68cbd-90af-4035-9efc-efd5164a0bd1', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:516e5445-d471-4ef8-b250-a906aeb59fce', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:dbccfd1a-dbf0-4c0f-ba05-d6dfb0bb3fb5', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:b46715f7-ee56-491f-8474-abf4783663a3', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:4484d2ee-4b2a-4624-882f-b003f30910b4', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:73265255-0896-4aa1-bb65-83cbd2a0fdac', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:f96b73ba-7eab-455e-b6d7-998b5d7cc155', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:008d4a94-60ec-4559-aabb-f052531dff16', '구분', '일자', '역명', '역번호'],
            ['/15060048/v1/uddi:8261933d-7231-4e66-87ed-e30d39a53de0', '구분', '일자', '역명', '역번호']]

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
            yyyymmdd = data[api[2]]
            station_name = data[api[3]]
            station_id = data[api[4]]

            for i, d in enumerate(data):
                if d.split('~'):
                    # 시간 형식인 경우
                    if len(d.split('~')) > 1:
                        start, end = int(d.split('~')[0]), int(d.split('~')[1])
                        # 24시인 경우는 0으로 들어가도록
                        if start == 24:
                            result_list.append([type, yyyymmdd, station_name, station_id, 0, data[d]])
                        else:
                            for time in range(start, end):
                                result_list.append([type, yyyymmdd, station_name, station_id, time, data[d]])
                if d.split('_'):
                    if len(d.split('_')) > 1:
                        start, end = int(d.split('_')[0]), int(d.split('_')[1])
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

gwangju = z.station_name.unique()

df = pd.read_excel('C:/Users/djoh1/Downloads/odsay_station.xlsx')
gwangju_df = df[df.lane_city == '광주'].station_name.unique()

for i in gwangju:
    if i not in gwangju_df:
        print('gwangju_df not containing:')
        print(i)
for i in gwangju_df:
    if i not in gwangju:
        print('gwangju not containing:')
        print(i)

gwangju[2] = '학동.증심사입구'
gwangju[14]='김대중컨벤션센터'
gwangju[20] = '학동.증심사입구'
gwangju[21] = '김대중컨벤션센터'
