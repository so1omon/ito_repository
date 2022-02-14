import cx_Oracle         # 오라클과 연동하는 패키지
import os
import pymysql             # mariadb와 연동하는 패키지
import pandas as pd


LOCATION = ".\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3"         # 오라클 연동하는 프로그램의 위치 필요.
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
OracleConnect = cx_Oracle.connect("silver", "silver", "192.168.20.13:1521/IDTCORA")       # 오라클 연동 정보입력
OracleCursor = OracleConnect.cursor()           #오라클 sql문 쓰기 위한 커서

conn=pymysql.connect(host='192.168.20.19', user='root', password='Azsxdc123$', db='connect', charset='utf8')       # mariadb 연동 정보입력
cur=conn.cursor()                # mariadb sql문 쓰기 위한 커서

oracleSql = f"""
select NVL(A.trg_emp_id, 'NULL') AS EMP_ID, A.appr_ymd, NVL(NVL(B.ymd, C.ymd), 'NULL') AS YMD, NVL(NVL(B.sta_hm, C.sta_hm), 'NULL') AS STA_HM, 
NVL(NVL(B.end_hm, C.end_hm),'NULL') AS END_HM, NVL(A.appl_type, 'NULL') AS TYPE, a.appl_id AS APPL_ID, NVL(NVL(b.del_yn, c.del_yn), 'NULL') AS DEL_YN, NVL(a.BF_APPL_ID, 'NULL') AS BF_APPL_ID
from ehr2011060.sy7010 A
left join (select appl_id, ymd, sta_hm, end_hm, del_yn
from ehr2011060.tam2215) B
on a.appl_id = b.appl_id
left join(select ymd, attend_cd, sta_hm, end_hm, appl_id, del_yn
from ehr2011060.tam5450) C
on a.appl_id = c.appl_id
where a.appl_stat_cd = '900' and (a.appl_type='1002' or a.appl_type='1004' or a.appl_type='1008' or a.appl_type='1010' or a.appl_type='1044')
"""                                          # 오라클 sql문 
OracleCursor.execute(oracleSql)

origin_table=pd.DataFrame()

for line in OracleCursor:
    data={'EMP_ID':line[0], 'APPR_YMD':line[1], 'YMD':line[2], 'STA_HM':line[3], 'END_HM':line[4], 'TYPE':line[5], 
        'APPL_ID':line[6], 'DEL_YN':line[7], 'BF_APPL_ID':line[8]}
    origin_table=origin_table.append(data,ignore_index=True)


bf_appl_id_list=[origin_table['BF_APPL_ID'].unique()]
# type_list=['1002','1008','1010','1044','1004']

# print(bf_appl_id_list)

origin_table = origin_table.drop(index=origin_table.loc[origin_table.BF_APPL_ID.isin(bf_appl_id_list)].index)
#1. bf_appl_list와 매칭되는 행 삭제

origin_table = origin_table.drop(index=origin_table.loc[origin_table.DEL_YN == 'Y'].index)
#2. DEL_YN이 Y인 행 삭제

origin_table = origin_table.drop(index=origin_table.loc[origin_table.BF_APPL_ID != 'NULL'].index)
#3. bf_appl_id가 NULL이 아닌 행 삭제

origin_table = origin_table.drop(index=origin_table.loc[(origin_table.TYPE=='1010') & (origin_table.YMD == 'NULL')].index)

# pd.set_option('display.max_row', 2000)
# 최대 출력 행 설정

cur.execute('SELECT emp_id FROM connect.hr_info')

column=['YMD', 'EMP_ID', 'DAYOFF1_TIME','DAYOFF1_ID','DAYOFF2_TIME','DAYOFF2_ID','DAYOFF3_TIME','DAYOFF3_ID','DAYOFF4_TIME','DAYOFF4_ID',
'OVER1_TIME','OVER1_ID','OVER2_TIME','OVER2_ID','OVER3_TIME','OVER3_ID','OVER4_TIME','OVER4_ID','BUSI_TRIP1_TIME','BUSI_TRIP1_ID',
'BUSI_TRIP2_TIME','BUSI_TRIP2_ID','BUSI_TRIP3_TIME','BUSI_TRIP3_ID','BUSI_TRIP4_TIME','BUSI_TRIP4_ID',
'HOME_ID','ETC_INFO','ETC_ID']


merge_table = pd.DataFrame(columns=column)

for emp_id in cur:#사번정보 로드
    data={
        'EMP_ID': emp_id[0]
    }
    merge_table=merge_table.append(data, ignore_index=True)

# print(origin_table.iloc[1])

temp_time='' #시작~끝 문자열 저장
dayoff_string_list=['DAYOFF1_TIME','DAYOFF2_TIME','DAYOFF3_TIME','DAYOFF4_TIME']
overtime_string_list=['OVER1_TIME','OVER2_TIME','OVER3_TIME','OVER4_TIME']
busitrip_string_list=['BUSI_TRIP1_TIME','BUSI_TRIP2_TIME','BUSI_TRIP3_TIME','BUSI_TRIP4_TIME']

origin_table.reset_index(inplace=True, drop=False)
print(origin_table)

for idx in range(len(origin_table)):
    rows_origin=origin_table.loc[idx] #origin table 행
    cond_emp_id=merge_table['EMP_ID']==rows_origin['EMP_ID']
    merge_index=merge_table.loc[cond_emp_id,'EMP_ID'].keys()[0] #merge table과 사번 일치하는 행 인덱스넘버

    if rows_origin['TYPE']=='1008': #초과근무
        if rows_origin['STA_HM']=='NULL':
            continue
        temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
        for isvalue in overtime_string_list:
            if merge_index.loc[merge_index][isvalue]:
                merge_table.at[merge_index,isvalue]=temp_time
                merge_table.at[merge_index,overtime_string_list.index(isvalue)]=rows_origin['APPL_ID']

    elif rows_origin['TYPE']=='1002': #연차
        if rows_origin['STA_HM']=='NULL':
            continue
        temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
        for isvalue in dayoff_string_list:
            if merge_index.loc[merge_index][isvalue]:
                merge_table.at[merge_index,isvalue]=temp_time
                merge_table.at[merge_index,dayoff_string_list.index(isvalue)]=rows_origin['APPL_ID']    

    elif rows_origin['TYPE']=='1010': #출장
        temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
        for isvalue in busitrip_string_list:
            if merge_index.loc[merge_index][isvalue]:
                merge_table.at[merge_index,isvalue]=temp_time
                merge_table.at[merge_index,busitrip_string_list.index(isvalue)]=rows_origin['APPL_ID']                
                

    # elif line.TYPE=='1002':#
    #     pass
    # elif line.TYPE=='1044':
    #     pass
    # elif line.TYPE=='1004':
    #     pass
    # elif line.TYPE=='1010':
    #     pass    
    # else:
    #     pass











# for line in DF:
#         sql =f"INSERT INTO `EHR_CAL` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#         cur.execute(sql,line)

# conn.commit()            # mariadb 변경사항 저장
# conn.close()            # mariadb 종료