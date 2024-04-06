import requests
import pandas as pd
import threading
from datetime import datetime
import time
import schedule
import pyodbc
import os
import traceback


### 에러 메세지 로그
def write_error_log(now ,place, err_loc, error_message):
    # 로그 파일을 저장할 디렉토리 경로 생성
    log_directory = 'C:/Temp/logs'
    
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # 로그 파일 경로 생성
    log_file_path = os.path.join(log_directory, 'error_log.txt')

    try:
        # 에러 메시지를 로그 파일에 추가
        with open(log_file_path, 'a') as log_file:
            log_file.write(f'{now} , {place} , {err_loc} , {error_message}' + '\n')
    except Exception as e:
        # 예외가 발생한 경우 표준 출력에 출력
        print("에러 로그를 기록하는 도중 예외가 발생했습니다:", str(e))
        # 추가적인 예외 로그를 콘솔에 출력
        traceback.print_exc()


############### 데이터베이스 연결 설정
def dbconnect ():
    server = 'database-1.c9so6u826z2w.ap-northeast-2.rds.amazonaws.com, 1433'
    database = 'hackathon'
    username = 'admin'
    password = 'qwer1234'
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    # cursor 생성 및 반환
    cursor = conn.cursor()
    return conn, cursor

############### API 불러오기
def bring_api (place):
    url = 'http://openapi.seoul.go.kr:8088/41514b514379697336387a6d784b58/json/citydata/1/1/{}'.format(place)
    now = pd.to_datetime('today')
    response = requests.get(url).json() # 나중에 예외처리 진행/ 로그 생성
    # 지역 불러오기
    AREA_NM = response["CITYDATA"]["AREA_NM"]
    AREA_CD = response["CITYDATA"]["AREA_CD"]
    return response, AREA_CD

############ 자전거 데이터 저장
def bike_info (now, AREA_CD,response,cursor,conn):
    sbike_response = response["CITYDATA"]["SBIKE_STTS"]
        
    for i in sbike_response:
        sbike_id = i["SBIKE_SPOT_ID"]
        sbike_cnt = i["SBIKE_PARKING_CNT"]

        cursor.execute("""INSERT INTO bike_history (CUR_TIME, AREA_CD, SBIKE_SPOT_ID, SBIKE_PARKING_CNT) 
                     VALUES (?, ?, ?, ?)""", 
                     (now, AREA_CD, sbike_id, int(sbike_cnt)))
        conn.commit()
        
############ 날씨 데이터 저장
def weather_info (now, AREA_CD,response,cursor,conn):
    weather_response = response["CITYDATA"]["WEATHER_STTS"][0]
    
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
    
 ############ 인구 데이터 저장  
def ppl_info (now, AREA_CD,response,cursor,conn):
    ppl_response = response["CITYDATA"]["LIVE_PPLTN_STTS"][0]
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

def get_citydata(place): 
    now = pd.to_datetime('today')
    try:
        err_loc = 'api'
        ############### api 호출  
        response, AREA_CD = bring_api (place)
        
        err_loc = 'db'
        ############### db 연결
        conn, cursor = dbconnect ()

        err_loc = 'bike'
        ############### 자전거 데이터    
        bike_info (now, AREA_CD,response,cursor,conn)
        
        err_loc = 'weather'    
        ############### 날씨 데이터
        weather_info (now, AREA_CD,response,cursor,conn)
        err_loc = 'ppl'    
        ############### 인구 데이터 
        ppl_info (now, AREA_CD,response,cursor,conn)
        ## db connection 닫기
        conn.close()
    except Exception as e:
        ## 에러 로그 저장
        write_error_log(now , place, err_loc, e)


 ############### 멀티쓰레드로 지역들 정보 끌어오기
def scheduling():
    places = ["가산디지털단지역", "구로디지털단지역", "강남역", "건대입구역", "고속터미널역", "교대역", 
            "선릉역","서울역","신도림역","신림역","역삼역","연신내역","왕십리역","용산역"]

    for place in places:
        t = threading.Thread(target=get_citydata, args=(place, ))
        t.start()
        
############### 1분 1회 데이터 추출
while True:
    scheduling()
    time.sleep(60)

