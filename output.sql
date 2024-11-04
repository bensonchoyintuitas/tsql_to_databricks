-- Converted on: 2024-11-04 21:41:20
-- Command: python convert_tsql_to_databricks.py input.sql output.sql

{{
    config(
        unique_key='ENCNTR_ID'
        ,alias='FIRSTNET_ED_LIST'
        ,materialized='table'
    )
}}



SELECT DISTINCT
    E.ENCNTR_ID AS ENCNTR_ID, 
    E.PERSON_ID AS PERSON_ID, 
    E.ENCNTR_FINANCIAL_ID,
    sTI.TRACKING_ID AS TRACKING_ID, 
    E.LAST_UTC_TS AS LAST_UTC_TS, 
    CAST(NULL AS TIMESTAMP) TI_LAST_UTC_TS, 
    CAST(NULL AS TIMESTAMP) TE_LAST_UTC_TS, 
    CAST(NULL AS TIMESTAMP) TC_LAST_UTC_TS, 
    CAST(NULL AS TIMESTAMP) TL_LAST_UTC_TS, 
    tg.Location_cd
    ,DATEADD(HOUR,10,current_timestamp()) DBT_RUN_TS
    ,{% if var("IM_ED_Hoursago" AS DBT_ARRIVAL_DATESTART,none) != none and var("IM_ED_Hoursagofrom",none) == 'maxlatest' %}
	DATEADD(HOUR, {{var("IM_ED_Hoursago",none)}} , (SELECT MAX(ARRIVE_DT_TM) FROM {{ source('IEMR','ENCOUNTER') }}  WHERE ARRIVE_DT_TM < (SELECT MAX(LAST_UTC_TS) FROM {{ source('IEMR','ENCOUNTER') }} )))
{% elif var("IM_ED_Hoursago",none) != none and var("IM_ED_Hoursagofrom",none) == 'sysdatetime' %}
	DATEADD(HOUR, {{var("IM_ED_Hoursago",none)}} , DATEADD(HOUR, 10, current_timestamp()))
{% elif var("IM_ED_Hoursago",none) != none and var("IM_ED_Hoursagofrom",none) == 'IM_ED_StartDate' %}
	CONVERT(TIMESTAMP(0), '{{ var("IM_ED_StartDate") }}')
{% else %}
	DATEADD(HOUR, -24 , DATEADD(HOUR, 10, current_timestamp()))
{% endif %}     


FROM {{source('IEMR','ENCOUNTER')}} E
     INNER JOIN 
     (
        SELECT 
        TI.ENCNTR_ID, 
        TI.TRACKING_ID,        
        ROW_NUMBER() OVER(PARTITION BY TI.ENCNTR_ID ORDER BY TI.LAST_UTC_TS DESC, TI.UPDT_DT_TM DESC) RK
        FROM {{source('IEMR','TRACKING_ITEM')}}TI
        WHERE TI.ACTIVE_IND = 1            
     ) sTI on e.ENCNTR_ID = sTI.ENCNTR_ID and sTI.RK =1
     INNER JOIN {{source('IEMR','TRACKING_CHECKIN')}}  TC ON TC.TRACKING_ID = sTI.TRACKING_ID
                                                                                AND TC.ACTIVE_IND = 1
     INNER JOIN  {{ref('__stg_06__IM_ED_S__FIRSTNET_ED_TRACK_GROUP')}} tg ON TC.TRACKING_GROUP_CD = tg.ED_TRACK_GROUP_CD
                                                                                                  AND E.LOC_FACILITY_CD = tg.Location_cd
     LEFT JOIN
     (
         SELECT EI.ENCNTR_ID
         FROM {{source('IEMR','ENCNTR_INFO')}} EI
              INNER JOIN  {{source('IEMR','CODE_VALUE')}} cvi ON EI.VALUE_CD = cvi.CODE_VALUE
                                                                                    AND cvi.CODE_SET = 100012
                                                                                    AND cvi.DISPLAY_KEY = 'CANCELLATIONOFENCOUNTER'
                                                                                    AND EI.ACTIVE_IND = 1
     ) sei ON sEI.ENCNTR_ID = E.ENCNTR_ID


WHERE  1=1


{% if var("IM_ED_Hoursago",none) != none and var("IM_ED_Hoursagofrom",none) == 'maxlatest' %}
	AND  E.ARRIVE_DT_TM >= DATEADD(HOUR, {{var("IM_ED_Hoursago",none)}} , (SELECT MAX(ARRIVE_DT_TM) FROM {{ source('IEMR','ENCOUNTER') }}  WHERE ARRIVE_DT_TM < (SELECT MAX(LAST_UTC_TS) FROM {{ source('IEMR','ENCOUNTER') }} )))
{% elif var("IM_ED_Hoursago",none) != none and var("IM_ED_Hoursagofrom",none) == 'sysdatetime' %}
	AND  E.ARRIVE_DT_TM >= DATEADD(HOUR, {{var("IM_ED_Hoursago",none)}} , current_timestamp())
{% elif var("IM_ED_Hoursago",none) != none and var("IM_ED_Hoursagofrom",none) == 'IM_ED_StartDate' %}
	AND  E.ARRIVE_DT_TM >= DATEADD(HOUR, -10, CONVERT(TIMESTAMP(0), '{{ var("IM_ED_StartDate") }}'))
{% else %}
	AND  E.ARRIVE_DT_TM >= DATEADD(HOUR, -24 , current_timestamp())
{% endif %}     

AND E.ACTIVE_IND = 1
AND DATEADD(HOUR, 10, E.BEG_EFFECTIVE_DT_TM) < DATEADD(HOUR, 10, current_timestamp())
AND DATEADD(HOUR, 10, E.END_EFFECTIVE_DT_TM) > DATEADD(HOUR, 10, current_timestamp())
AND sei.encntr_id IS NULL --- encouner not cancelled
