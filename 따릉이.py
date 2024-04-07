import requests
import pandas as pd
import threading
import time
import os
import traceback
from dbmodule import dbConnect, CommitData



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



############### API 불러오기
def bring_api (place):
    url = 'http://openapi.seoul.go.kr:8088/41514b514379697336387a6d784b58/json/citydata/1/1/{}'.format(place)
    response = requests.get(url).json() # 나중에 예외처리 진행/ 로그 생성
    # 지역 불러오기
    AREA_NM = response["CITYDATA"]["AREA_NM"]
    AREA_CD = response["CITYDATA"]["AREA_CD"]
    return response, AREA_CD


def get_citydata(place): 
    now = pd.to_datetime('today')
    try:
        err_loc = 'api'
        ############### api 호출  
        response, AREA_CD = bring_api (place)
        
        err_loc = 'db'
        ############### db 연결
        conn, cursor = dbConnect.dbconnect ()

        err_loc = 'bike'
        ############### 자전거 데이터    
        CommitData.bike_info (now, AREA_CD,response,cursor,conn)
        
        err_loc = 'weather'    
        ############### 날씨 데이터
        CommitData.weather_info (now, AREA_CD,response,cursor,conn)
        err_loc = 'ppl'    
        ############### 인구 데이터 
        CommitData.ppl_info (now, AREA_CD,response,cursor,conn)
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

