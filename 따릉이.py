import requests
import pandas as pd
import time
import threading

def get_citydata(place): 
    url = 'http://openapi.seoul.go.kr:8088/41514b514379697336387a6d784b58/json/citydata/1/1/{}'.format(place)

    try:
        now = pd.to_datetime('today')
        date = now.strftime('%m_%d')
        time = now.strftime('%H:%M:%S')

        response = requests.get(url).json()

        area_name = response["CITYDATA"]["AREA_NM"]
        area_code = response["CITYDATA"]["AREA_CD"]
        sbike_response = response["CITYDATA"]["SBIKE_STTS"]

        add = pd.DataFrame(None, columns=["시간", "대여소 코드", "카운트"])

        for i in sbike_response:
            sbike_id = i["SBIKE_SPOT_ID"]
            sbike_cnt = i["SBIKE_PARKING_CNT"]

            add.loc[len(add)] = [time, sbike_id, sbike_cnt]
            
        # except 분기점: 기존의 파일 불러와서 새로운 데이터 추가 후 파일 덮어쓰기
        origin = pd.read_csv(f"./output/따릉이/{date} {area_name} 따릉이 현황.csv")
            
        result = pd.concat([origin, add], axis = 0).reset_index(drop=True)
        
        result.to_csv(f"./output/따릉이/{date} {area_name} 따릉이 현황.csv", index=False, encoding='utf-8-sig')

        print("{} 호출 성공".format(place))

    except FileNotFoundError as e:
        # except 분기점: 일 단위 파일 최초 생성 시 파일이 없어 불러오기 실패
        add.to_csv(f"./output/따릉이/{date} {area_name} 따릉이 현황.csv", index=False, encoding='utf-8-sig')
        print("{} 신규 생성".format(place))

start = time.time()

places = ["가산디지털단지역", "구로디지털단지역", "강남역", "건대입구역", "고속터미널역", "교대역", "선릉역","서울역","신도림역","신림역","신촌·이대역","역삼역","연신내역","왕십리역","용산역"]

# threads = []
for place in places:
    t = threading.Thread(target=get_citydata, args=(place, ))
    t.start()

#     threads.append(t)

# for t in threads:
#     t.join()  # 스레드가 종료될 때까지 대기

end = time.time()

print("수행시간: %f 초" % (end - start))