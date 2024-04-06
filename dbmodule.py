import pyodbc
import pandas as pd


############### 데이터베이스 연결 설정
class dbConnect:
    def dbconnect ():
        server = 'database-1.c9so6u826z2w.ap-northeast-2.rds.amazonaws.com, 1433'
        database = 'hackathon'
        username = 'admin'
        password = 'qwer1234'
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        # cursor 생성 및 반환
        cursor = conn.cursor()
        return conn, cursor

#region 데이터 저장
class CommitData:
    ############ 자전거 데이터 저장
    @staticmethod
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
    @staticmethod
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
    @staticmethod 
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
#/region 데이터 저장

class getData :
############ SP : 자전거 정보 불러오기
    @staticmethod
    def get_station_info_by_sbike_spot_id (conn, bike_station_code,start_dt, end_dt):
        # 만약 시작일이 비워져있으면, 2024년 04월 06일로 자동 세팅
        if start_dt =='':
            start_dt = '2024-04-06'   
        if end_dt == '':
            return     
        sql_query = f"""select bh.cur_time, bh.area_cd , bh.sbike_spot_id,bh.sbike_parking_cnt
                ,wi.WEATHER_TIME, wi.TEMP, wi.SENSIBLE_TEMP, wi.HUMIDITY,wi.WIND_DIRCT,wi.WIND_SPD
                ,wi.PRECIPITATION, wi.PRECPT_TYPE, wi.SUNRISE, wi.SUNSET, wi.UV_INDEX_LVL, wi.UV_INDEX
                , wi.PM25_INDEX, wi.PM25,wi.PM10_INDEX , wi.PM10, wi.AIR_IDX, wi.AIR_IDX_MVL, wi.AIR_IDX_MAIN
                ,ppi.PPLTN_TIME ,ppi.AREA_CONGEST_LVL ,ppi.AREA_PPLTN_MIN ,ppi.AREA_PPLTN_MAX ,ppi.MALE_PPLTN_RATE 
                ,ppi.FEMALE_PPLTN_RATE , ppi.PPLTN_RATE_0 , ppi.PPLTN_RATE_10 , ppi.PPLTN_RATE_20 , ppi.PPLTN_RATE_30 
                , ppi.PPLTN_RATE_40 , ppi.PPLTN_RATE_50 , ppi.PPLTN_RATE_60 , ppi.PPLTN_RATE_70 , ppi.RESNT_PPLTN_RATE 
                , ppi.NON_RESNT_PPLTN_RATE 
            from bike_history bh with (nolock)
            left join weather_info wi with (nolock) on bh.area_cd = wi.area_cd and bh.cur_time = wi.cur_time
            left join ppl_info ppi with (nolock) on bh.area_cd = ppi.area_cd and bh.cur_time = ppi.cur_time
            where bh.sbike_spot_id = '{bike_station_code}' 
            and bh.cur_time >= '{start_dt}'
            and bh.cur_time <'{end_dt}' ;"""
        df = pd.read_sql(sql_query,conn) 
        return df
