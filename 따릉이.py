import requests
import pandas as pd
import threading
from datetime import datetime
import time
import schedule
import pyodbc



def get_citydata(place): 
    ############### 데이터베이스 연결 설정
    server = 'database-1.c9so6u826z2w.ap-northeast-2.rds.amazonaws.com, 1433'
    database = 'hackathon'
    username = 'admin'
    password = 'qwer1234'
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

    ############### 커서 생성
    cursor = conn.cursor()
    
    ############### API 불러오기
    url = 'http://openapi.seoul.go.kr:8088/41514b514379697336387a6d784b58/json/citydata/1/1/{}'.format(place)
    now = pd.to_datetime('today')
    response = requests.get(url).json() # 나중에 예외처리 진행/ 로그 생성

    ############### 지역 불러오기
    AREA_NM = response["CITYDATA"]["AREA_NM"]
    AREA_CD = response["CITYDATA"]["AREA_CD"]
    
    ############### 자전거 데이터    
    sbike_response = response["CITYDATA"]["SBIKE_STTS"]
        
    for i in sbike_response:
        sbike_id = i["SBIKE_SPOT_ID"]
        sbike_cnt = i["SBIKE_PARKING_CNT"]

        cursor.execute("""INSERT INTO bike_history (CUR_TIME, AREA_CD, SBIKE_SPOT_ID, SBIKE_PARKING_CNT) 
                     VALUES (?, ?, ?, ?)""", 
                     (now, AREA_CD, sbike_id, int(sbike_cnt)))
        conn.commit()
        
    ############### 날씨 데이터
    weather_response = response["CITYDATA"]["WEATHER_STTS"][0]
    ppl_response = response["CITYDATA"]["LIVE_PPLTN_STTS"][0]
    
    cursor.execute("""
        INSERT INTO weather_info 
        VALUES 
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
        now
        , AREA_CD
        , weather_response["WEATHER_TIME"]
        , float(weather_response["TEMP"])
        , float(weather_response["SENSIBLE_TEMP"])
        # , weather_response["MAX_TEMP"]
        # , weather_response["MIN_TEMP"]
        , float(weather_response["HUMIDITY"])
        , weather_response["WIND_DIRCT"]
        , float(weather_response["WIND_SPD"])
        , weather_response["PRECIPITATION"]
        , weather_response["PRECPT_TYPE"]
        , weather_response["SUNRISE"]
        , weather_response["SUNSET"]
        , weather_response["UV_INDEX_LVL"]
        ,weather_response["UV_INDEX"]
        , weather_response["PM25_INDEX"]
        , float(weather_response["PM25"])
        , weather_response["PM10_INDEX"]
        , float(weather_response["PM10"])
        , weather_response["AIR_IDX"]
        , float(weather_response["AIR_IDX_MVL"])
        , weather_response["AIR_IDX_MAIN"]
        ))
    conn.commit()
    ############### 인구 데이터 
    cursor.execute("""
        INSERT INTO ppl_info 
        VALUES 
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
        now
        , AREA_CD
        ,ppl_response["PPLTN_TIME"]
        , ppl_response["AREA_CONGEST_LVL"]
        , float(ppl_response["AREA_PPLTN_MIN"])
        , float(ppl_response["AREA_PPLTN_MAX"])
        , float(ppl_response["MALE_PPLTN_RATE"])
        , float(ppl_response["FEMALE_PPLTN_RATE"])
        , float(ppl_response["PPLTN_RATE_0"])
        , float(ppl_response["PPLTN_RATE_10"])
        , float(ppl_response["PPLTN_RATE_20"])
        , float(ppl_response["PPLTN_RATE_30"])
        , float(ppl_response["PPLTN_RATE_40"])
        , float(ppl_response["PPLTN_RATE_50"])
        , float(ppl_response["PPLTN_RATE_60"])
        , float(ppl_response["PPLTN_RATE_70"])
        , float(ppl_response["RESNT_PPLTN_RATE"])
        , float(ppl_response["NON_RESNT_PPLTN_RATE"] ))   )    
    conn.commit()
    ## connection 닫기
    conn.close()

 ############### 멀티쓰레드로 지역들 정보 끌어오기
def scheduling():
    places = ["가산디지털단지역", "구로디지털단지역", "강남역", "건대입구역", "고속터미널역", "교대역", 
            "선릉역","서울역","신도림역","신림역","신촌·이대역","역삼역","연신내역","왕십리역","용산역"]

    for place in places:
        t = threading.Thread(target=get_citydata, args=(place, ))
        t.start()
        
############### 1분 1회 데이터 추출
while True:
    scheduling()
    time.sleep(60)

