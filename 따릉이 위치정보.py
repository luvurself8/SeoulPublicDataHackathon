import requests
import pandas as pd

places = ["가산디지털단지역", "구로디지털단지역", "강남역", "건대입구역", "고속터미널역", "교대역", 
        "선릉역","서울역","신도림역","신림역","신촌·이대역","역삼역","연신내역","왕십리역","용산역"]

print(len(places))

sbike_df = pd.DataFrame(None, columns=["SBIKE_SPOT_ID", "AREA_CD", "SBIKE_SPOT_NM", "SBIKE_X", "SBIKE_Y"])

for place in places:
    
    print(place)
    
    url = f'http://openapi.seoul.go.kr:8088/41514b514379697336387a6d784b58/json/citydata/1/1/{place}'

    response = requests.get(url).json()

    area_code = response["CITYDATA"]["AREA_CD"]
    sbike_response = response["CITYDATA"]["SBIKE_STTS"]

    for i in sbike_response:
        temp_dict = {"SBIKE_SPOT_ID" : i["SBIKE_SPOT_ID"],
                    "AREA_CD" : area_code, 
                    "SBIKE_SPOT_NM" : i["SBIKE_SPOT_NM"], 
                    "SBIKE_X" : i["SBIKE_X"], 
                    "SBIKE_Y" : i["SBIKE_Y"]}
        
        temp_df = pd.DataFrame([temp_dict])
        
        print(temp_df.shape)
        
        sbike_df = pd.concat([sbike_df, temp_df], axis = 0)

sbike_df.to_csv("따릉이 위치정보.csv", index=False, encoding="utf-8-sig")