import requests
import pandas as pd
import threading

import time
import schedule

# 반복시킬 전역변수 설정
CNT = 0

def get_citydata(place): 
    url = 'http://openapi.seoul.go.kr:8088/41514b514379697336387a6d784b58/json/citydata/1/1/{}'.format(place)
    
    now = pd.to_datetime('today')
    date = now.strftime('%m_%d')
    time = now.strftime('%H:%M:%S')

    response = requests.get(url).json()

    area_name = response["CITYDATA"]["AREA_NM"]
    area_code = response["CITYDATA"]["AREA_CD"]
    
    # 자전거 데이터    
    sbike_response = response["CITYDATA"]["SBIKE_STTS"]

    sbike_new = pd.DataFrame(None, columns=["AREA_CD", "시간", "대여소 코드", "카운트"])
        
    for i in sbike_response:
        sbike_id = i["SBIKE_SPOT_ID"]
        sbike_cnt = i["SBIKE_PARKING_CNT"]
        sbike_new.loc[len(sbike_new)] = [area_code, time, sbike_id, sbike_cnt]
        
    try:        
        # except 분기점: 기존의 파일 불러와서 새로운 데이터 추가 후 파일 덮어쓰기
        sbike_origin = pd.read_csv(f"./output/따릉이/{date} {area_name} 따릉이 현황.csv")
        sbike_result = pd.concat([sbike_origin, sbike_new], axis = 0).reset_index(drop=True)
        sbike_result.to_csv(f"./output/따릉이/{date} {area_name} 따릉이 현황.csv", index=False, encoding='utf-8-sig')

        print("{} 따릉이 데이터 호출 성공".format(place))

    except FileNotFoundError as e:
        # except 분기점: 일 단위 파일 최초 생성 시 파일이 없어 불러오기 실패
        sbike_new.to_csv(f"./output/따릉이/{date} {area_name} 따릉이 현황.csv", index=False, encoding='utf-8-sig')
        print("{} 따릉이 데이터 신규 생성".format(place))
        
    # 날씨 및 인구 데이터
    weather_response = response["CITYDATA"]["WEATHER_STTS"][0]
    ppl_response = response["CITYDATA"]["LIVE_PPLTN_STTS"][0]
    
    local_data = {
    "WEATHER_TIME": [weather_response["WEATHER_TIME"]],
    "TEMP": [weather_response["TEMP"]],
    "SENSIBLE_TEMP": [weather_response["SENSIBLE_TEMP"]],
    "MAX_TEMP": [weather_response["MAX_TEMP"]],
    "MIN_TEMP": [weather_response["MIN_TEMP"]],
    "HUMIDITY": [weather_response["HUMIDITY"]],
    "WIND_DIRCT": [weather_response["WIND_DIRCT"]],
    "WIND_SPD": [weather_response["WIND_SPD"]],
    "PRECIPITATION": [weather_response["PRECIPITATION"]],
    "PRECPT_TYPE": [weather_response["PRECPT_TYPE"]],
    "PCP_MSG": [weather_response["PCP_MSG"]],
    "SUNRISE": [weather_response["SUNRISE"]],
    "SUNSET": [weather_response["SUNSET"]],
    "UV_INDEX_LVL": [weather_response["UV_INDEX_LVL"]],
    "UV_INDEX": [weather_response["UV_INDEX"]],
    "UV_MSG": [weather_response["UV_MSG"]],
    "PM25_INDEX": [weather_response["PM25_INDEX"]],
    "PM25": [weather_response["PM25"]],
    "PM10_INDEX": [weather_response["PM10_INDEX"]],
    "PM10": [weather_response["PM10"]],
    "AIR_IDX": [weather_response["AIR_IDX"]],
    "AIR_IDX_MVL": [weather_response["AIR_IDX_MVL"]],
    "AIR_IDX_MAIN": [weather_response["AIR_IDX_MAIN"]],
    "AIR_MSG": [weather_response["AIR_MSG"]],
    "PPLTN_TIME": [ppl_response["PPLTN_TIME"]],
    "AREA_CONGEST_LVL": [ppl_response["AREA_CONGEST_LVL"]],
    "AREA_CONGEST_MSG": [ppl_response["AREA_CONGEST_MSG"]],
    "AREA_PPLTN_MIN": [ppl_response["AREA_PPLTN_MIN"]],
    "AREA_PPLTN_MAX": [ppl_response["AREA_PPLTN_MAX"]],
    "MALE_PPLTN_RATE": [ppl_response["MALE_PPLTN_RATE"]],
    "PPLTN_RATE_0": [ppl_response["PPLTN_RATE_0"]],
    "PPLTN_RATE_10": [ppl_response["PPLTN_RATE_10"]],
    "PPLTN_RATE_20": [ppl_response["PPLTN_RATE_20"]],
    "PPLTN_RATE_30": [ppl_response["PPLTN_RATE_30"]],
    "PPLTN_RATE_40": [ppl_response["PPLTN_RATE_40"]],
    "PPLTN_RATE_50": [ppl_response["PPLTN_RATE_50"]],
    "PPLTN_RATE_60": [ppl_response["PPLTN_RATE_60"]],
    "PPLTN_RATE_70": [ppl_response["PPLTN_RATE_70"]],
    "RESNT_PPLTN_RATE": [ppl_response["RESNT_PPLTN_RATE"]],
    "NON_RESNT_PPLTN_RATE": [ppl_response["NON_RESNT_PPLTN_RATE"]]
    }

    # DataFrame 생성
    weather_ppl_new = pd.DataFrame(local_data)
    
    try:        
        # except 분기점: 기존의 파일 불러와서 새로운 데이터 추가 후 파일 덮어쓰기
        weather_ppl_origin = pd.read_csv(f"./output/local/{date} {area_name} 현황.csv")
            
        weather_ppl_result = pd.concat([weather_ppl_origin, weather_ppl_new], axis = 0).reset_index(drop=True)
        
        weather_ppl_result.to_csv(f"./output/local/{date} {area_name} 현황.csv", index=False, encoding='utf-8-sig')

        print("{} 도시데이터 호출 성공".format(place))
            
    except FileNotFoundError as e:
        # except 분기점: 일 단위 파일 최초 생성 시 파일이 없어 불러오기 실패
        weather_ppl_new.to_csv(f"./output/local/{date} {area_name} 현황.csv", index=False, encoding='utf-8-sig')
        print("{} 도시데이터 신규 생성".format(place))

def scheduling():
    print("데이터 수집 시작")

    places = ["가산디지털단지역", "구로디지털단지역", "강남역", "건대입구역", "고속터미널역", "교대역", 
            "선릉역","서울역","신도림역","신림역","신촌·이대역","역삼역","연신내역","왕십리역","용산역"]

    # threads = []
    for place in places:
        t = threading.Thread(target=get_citydata, args=(place, ))
        t.start()

    #     threads.append(t)

    # for t in threads:
    #     t.join()  # 스레드가 종료될 때까지 대기

    # 전역변수 접근
    global CNT
    CNT += 1

# 30초에 한번씩 데이터 수집 진행    
job = schedule.every(30).seconds.do(scheduling)

# scheduling 하기 전 함수 최초 실행
scheduling()

while True:
    print("루프 시작")
    
    # schedule.run_pending(): 대기만 걸어두고 시간이 지나야 처음 함수 실행
    schedule.run_pending()
    print(schedule.jobs)
    
    # time.sleep(): 데이터 수집 주기에 맞춰서 대기 시간 설정
    time.sleep(30)

    # 반복횟수 설정
    if CNT == 3:
        break
