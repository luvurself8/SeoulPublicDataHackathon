import requests
import pandas as pd
import threading
import time
from dbmodule import dbConnect, CommitData ,Logs


############### API 불러오기
def bring_api (place):
    url = 'http://openapi.seoul.go.kr:8088/41514b514379697336387a6d784b58/json/citydata/1/1/{}'.format(place)
    response = requests.get(url).json() # 나중에 예외처리 진행/ 로그 생성
    # 지역 불러오기
    #AREA_NM = response["CITYDATA"]["AREA_NM"]
    AREA_CD = response["CITYDATA"]["AREA_CD"]
    return response, AREA_CD


def get_citydata(place): 
    now = pd.to_datetime('today')
    
    try:
        ############### api 호출  
        response, AREA_CD = bring_api (place , now)
        ############### db 연결
        conn, cursor = dbConnect.dbconnect (now)

        ############### 자전거 데이터    
        CommitData.bike_info (now, AREA_CD,response,cursor,conn)
           
        ############### 인구 데이터 
        CommitData.ppl_info (now, AREA_CD,response,cursor,conn)
           
        ############### 날씨 데이터
        CommitData.weather_info (now, AREA_CD,response,cursor,conn)
        
    except Exception as e:
        err_loc = 'api or db'
        Logs.write_error_log(now, AREA_CD, err_loc, e, '')
        
    finally :
        if conn is not None:
            ## db connection 닫기
            conn.close()


 ############### 멀티쓰레드로 지역들 정보 끌어오기
def scheduling():
    places = ["가산디지털단지역", "구로디지털단지역", "강남역", "건대입구역", "고속터미널역", "교대역", 
            "선릉역","서울역","신도림역","신림역","역삼역","연신내역","왕십리역","용산역"]

    for place in places:
        t = threading.Thread(target=get_citydata, args=(place, ))
        t.start()
        
############### 1분 1회 데이터 추출
while True:
    try :
        scheduling()
    except Exception as e :
        print(e)
    finally :
        time.sleep(60)