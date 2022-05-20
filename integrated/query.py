# 초기 신청정보 가져오기
oracle_get_appl_sql=""" 
select NVL(A.trg_emp_id, 'None') AS EMP_ID, A.appr_ymd, NVL(NVL(B.ymd, C.ymd), 'None') AS YMD, 
NVL(NVL(B.sta_hm, C.sta_hm), 'None') AS STA_HM, NVL(NVL(B.end_hm, C.end_hm),'None') AS END_HM, 
NVL(A.appl_type, 'None') AS TYPE, a.appl_id AS APPL_ID, NVL(NVL(b.del_yn, c.del_yn), 'None') AS DEL_YN, 
NVL(a.BF_APPL_ID, 'None') AS BF_APPL_ID, a.appl_txt as APPL_TXT, NVL(B.reward_type, 'None') AS REWARD_TYPE,
NVL(B.RSN, 'None') as RSN from ehr2011060.sy7010 A
left join(
    select appl_id, ymd, sta_hm, end_hm, del_yn, reward_type, rsn
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
)
order by EMP_ID
"""

pymysql_get_hr_info='SELECT emp_id, emp_nm, org_nm FROM connect.hr_info'

oracle_insert_table="SELECT EMP_ID,SHIFT_CD,WORK_TYPE FROM EHR2011060.TAM5400_V WHERE YMD =(SELECT TO_CHAR(SYSDATE-{0}, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)"


oracle_get_ehr_con_sql = """
SELECT B.EMP_ID AS emp_id,NVL(B.CD18, '0') AS over_std_time, NVL(D.CNT, '0') AS dayoff_std_time, 
NVL(D.CNT-E.VAL, D.CNT) AS dayoff_rest_time,
NVL(F.EHC, '30') AS p_edu_std_time, NVL(G.E_SUM, '0') AS p_edu_admit_time, NVL(H.EHC, '20') AS i_edu_std_time, 
NVL(I.E_SUM, '0') AS i_edu_admit_time
FROM EHR2011060.PA1010 A 
left join (
    SELECT EMP_ID , T2.COND_CD18 AS CD18
    FROM EHR2011060.PA1020 T1,EHR2011060.SY5020 T2 
    WHERE T1.C_CD = '2011060'AND (T1.APPNT_NM!='퇴직' AND  T1.APPNT_NM!='파견계약해지') AND (SELECT TO_CHAR(SYSDATE, 'YYYYMMDD')FROM DUAL
) BETWEEN T1.STA_YMD AND T1.END_YMD
            AND T1.LAST_YN = 'Y'
            AND T2.C_CD = T1.C_CD
          AND T2.IDX_CD = '/SY03'
          AND T2.CD = T1.EMP_GRADE_CD) B
          ON A.EMP_ID=B.EMP_ID
left join (
    SELECT EMP_ID, NVL(SUM(FLOOR(VAL))+TRUNC(SUM(MOD(VAL,1)*100)/60)+MOD(SUM(MOD(VAL,1)*100),60)/100,0) AS C_SUM 
    FROM EHR2011060.TAM5410 
    WHERE SUBSTR(YMD, 1, 6) = (SELECT TO_CHAR(SYSDATE, 'YYYYMM')FROM DUAL) AND (LABOR_CD = 'P010' OR LABOR_CD = 'P020' OR LABOR_CD = 'P030' OR LABOR_CD = 'P040' OR LABOR_CD = 'P050') GROUP BY EMP_ID
) C ON A.EMP_ID=C.EMP_ID
LEFT JOIN (
    (SELECT EMP_ID,(SUM(CRT_D_CNT)-NVL(SUM(USED_D_CNT),0)) AS CNT
    FROM EHR2011060.TAM6030 
    WHERE HOLI_CLASS = '0001' AND SUBSTR(EMP_ID,5,1)!='4'  AND (SUBSTR(USE_END_YMD, 1, 4) = (SELECT TO_CHAR(SYSDATE, 'YYYY')FROM DUAL) OR  SUBSTR(USE_END_YMD, 1, 4) = (SELECT TO_CHAR(SYSDATE, 'YYYY')FROM DUAL)+1)GROUP BY EMP_ID)
    UNION
    (SELECT EMP_ID,(SUM(CRT_D_CNT)) AS CNT
    FROM EHR2011060.TAM6030 
    WHERE HOLI_CLASS = '0001' AND SUBSTR(EMP_ID,5,1)='4' GROUP BY EMP_ID)
) D ON A.EMP_ID = D.EMP_ID
LEFT JOIN (
    WITH Z as(
        select emp_id as EMP_ID, ymd, SUM(CASE WHEN D_H_TYPE='H' THEN VAL ELSE 0 END)+COUNT(CASE WHEN D_H_TYPE='D' THEN '1' END)*8 AS TOTAL 
        from ehr2011060.tam5450
        WHERE ATTEND_CD = '0001' AND DEL_YN = 'N'
        group by emp_id, ymd
        )
        select EMP_ID,sum(TOTAL) as VAL from Z 
        inner join(
            SELECT EMP_ID AS B_EMP_ID,MIN(USE_STA_YMD) AS USE_STA_YMD,MAX(USE_END_YMD)AS USE_END_YMD
            FROM EHR2011060.TAM6030 
            WHERE HOLI_CLASS = '0001' 
            AND SUBSTR(EMP_ID,5,1)!='4'  
            AND SUBSTR(USE_END_YMD, 1, 4) = (SELECT TO_CHAR(SYSDATE, 'YYYY')FROM DUAL)
            group by EMP_ID
        )B on Z.EMP_ID = B.B_EMP_ID AND Z.YMD>=B.USE_STA_YMD AND Z.YMD<=B.USE_END_YMD GROUP BY EMP_ID
    UNION
    (SELECT EMP_ID,
    SUM(CASE WHEN D_H_TYPE='H' THEN VAL ELSE 0 END)+COUNT(CASE WHEN D_H_TYPE='D' THEN '1' END)*8 AS VAL
    FROM EHR2011060.TAM5450 
    WHERE ATTEND_CD = '0001' AND DEL_YN = 'N' AND SUBSTR(EMP_ID,5,1)='4'  
    GROUP BY EMP_ID)
)E ON A.EMP_ID = E.EMP_ID
LEFT JOIN (
    SELECT EMP_ID, EXCE_H_CNT AS EHC 
    FROM EHR2011060.C2011060_TE2060 
    WHERE EDU_GRP_CD = '20' AND STA_YY = (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL)
) F ON A.EMP_ID = F.EMP_ID
LEFT JOIN (
    SELECT EMP_ID, SUM(APPR_H_CNT) AS E_SUM 
    FROM EHR2011060.TE2040 A 
    LEFT JOIN( 
        SELECT EDU_CURRI_ID, EDU_REG_CD ERG FROM EHR2011060.TE3020
    ) B ON A.EDU_CURRI_ID = B.EDU_CURRI_ID WHERE SUBSTR(END_YMD, 1, 4) = (SELECT TO_CHAR(SYSDATE, 'YYYY')FROM DUAL) AND B.ERG = '10' GROUP BY EMP_ID
) G ON A.EMP_ID = G.EMP_ID
LEFT JOIN (
    SELECT EMP_ID, EXCE_H_CNT AS EHC 
    FROM EHR2011060.C2011060_TE2060 
    WHERE EDU_GRP_CD = '10' AND STA_YY = (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL)
) H ON A.EMP_ID = H.EMP_ID
LEFT JOIN (
    SELECT EMP_ID, SUM(APPR_H_CNT) AS E_SUM 
    FROM EHR2011060.TE2040 A 
    LEFT JOIN( 
        SELECT EDU_CURRI_ID, EDU_REG_CD ERG 
        FROM EHR2011060.TE3020
    )B ON A.EDU_CURRI_ID = B.EDU_CURRI_ID WHERE SUBSTR(END_YMD, 1, 4) = (SELECT TO_CHAR(SYSDATE, 'YYYY')FROM DUAL) AND B.ERG = '20' GROUP BY EMP_ID
) I ON A.EMP_ID = I.EMP_ID
WHERE NOT B.EMP_ID is NULL
"""

pymysql_get_ehr_con_sql='''
    SELECT emp_id, `NAME`, SUM(
        CASE WHEN cal_overtime!='0000' then cast(cast(cal_overtime AS INT)/100 as int)*60 + 
        mod(cast(cal_overtime AS INT), 100) ELSE 0 END) 
    AS TOTAL_OVERTIME FROM connect.ehr_cal 
    WHERE ymd>={0} AND ymd<={1} GROUP BY emp_id, `NAME`;
'''