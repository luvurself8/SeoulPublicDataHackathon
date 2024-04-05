--drop table bike_history
--drop table weather_info
--drop table ppl_info

--select * from bike_history with (nolock)
create table bike_history
(
CUR_TIME datetime
, AREA_CD nvarchar(10)
, SBIKE_SPOT_ID nvarchar(10)
, SBIKE_PARKING_CNT int
)

--select * from weather_info with (nolock)

create table weather_info

(
CUR_TIME datetime
, AREA_CD nvarchar(10)
,WEATHER_TIME datetime
,TEMP float
,SENSIBLE_TEMP float
,HUMIDITY float
,WIND_DIRCT nvarchar(10)
,WIND_SPD float
, PRECIPITATION nvarchar(10)
, PRECPT_TYPE nvarchar(10)
, SUNRISE time
, SUNSET time
, UV_INDEX_LVL nvarchar(10)
, UV_INDEX nvarchar(10)
, PM25_INDEX nvarchar(10)
, PM25 float
, PM10_INDEX nvarchar(10)
, PM10 float
, AIR_IDX nvarchar(10)
, AIR_IDX_MVL float
, AIR_IDX_MAIN nvarchar(10)
)



create table ppl_info

(
CUR_TIME datetime
, AREA_CD nvarchar(10)
,PPLTN_TIME datetime
,AREA_CONGEST_LVL nvarchar(10)
,AREA_PPLTN_MIN float
,AREA_PPLTN_MAX float
,MALE_PPLTN_RATE float
,FEMALE_PPLTN_RATE float
, PPLTN_RATE_0 float
, PPLTN_RATE_10 float
, PPLTN_RATE_20 float
, PPLTN_RATE_30 float
, PPLTN_RATE_40 float
, PPLTN_RATE_50 float
, PPLTN_RATE_60 float
, PPLTN_RATE_70 float
, RESNT_PPLTN_RATE float
, NON_RESNT_PPLTN_RATE float
)
--select * from ppl_info with (nolock)