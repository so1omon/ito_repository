# 초기 신청정보 가져오기
oracle_get_appl_sql=""" 
select NVL(A.trg_emp_id, 'NULL') AS EMP_ID, A.appr_ymd, NVL(NVL(B.ymd, C.ymd), 'NULL') AS YMD, 
NVL(NVL(B.sta_hm, C.sta_hm), 'NULL') AS STA_HM, NVL(NVL(B.end_hm, C.end_hm),'NULL') AS END_HM, 
NVL(A.appl_type, 'NULL') AS TYPE, a.appl_id AS APPL_ID, NVL(NVL(b.del_yn, c.del_yn), 'NULL') AS DEL_YN, 
NVL(a.BF_APPL_ID, 'NULL') AS BF_APPL_ID, a.appl_txt as APPL_TXT, NVL(B.reward_type, 'NULL') AS REWARD_TYPE
from ehr2011060.sy7010 A
left join(
    select appl_id, ymd, sta_hm, end_hm, del_yn, reward_type
    from ehr2011060.tam2215
) B on a.appl_id = b.appl_id
left join(
    select ymd, attend_cd, sta_hm, end_hm, appl_id, del_yn
    from ehr2011060.tam5450
) C
on a.appl_id = c.appl_id
where a.appl_stat_cd = '900' and (
    ( 
        (
            a.appl_type='1002' or a.appl_type='1004' or a.appl_type='1008' or a.appl_type='1010'
        ) 
        and NVL(B.ymd, C.ymd)=(
            SELECT TO_CHAR(SYSDATE-{0}, 'YYYYMMDD')AS YYYYMMDD FROM DUAL
        )
    ) 
    or a.appl_type='1044' and substr(a.appl_txt,5,10)=(
        SELECT TO_CHAR(SYSDATE-{0}, 'YYYY.MM.DD')AS YYYYMMDD FROM DUAL
    )
)"""

pymysql_get_hr_info='SELECT emp_id, emp_nm, org_nm FROM connect.hr_info'