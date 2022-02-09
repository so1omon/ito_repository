import cx_Oracle         # 오라클과 연동하는 패키지
import os
import pymysql             # mariadb와 연동하는 패키지
import pandas as pd
from datetime import datetime


LOCATION = "..\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3"         # 오라클 연동하는 프로그램의 위치 필요.
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
OracleConnect = cx_Oracle.connect("silver", "silver", "192.168.20.13:1521/IDTCORA")       # 오라클 연동 정보입력
OracleCursor = OracleConnect.cursor()           #오라클 sql문 쓰기 위한 커서

conn=pymysql.connect(host='192.168.20.19', user='root', password='Azsxdc123$', db='connect', charset='utf8')       # mariadb 연동 정보입력
cur=conn.cursor()                # mariadb sql문 쓰기 위한 커서

oracleSql = f"""
select NVL(A.trg_emp_id, 'NULL') AS EMP_ID, A.appr_ymd, NVL(NVL(B.ymd, C.ymd), 'NULL') AS YMD, NVL(NVL(B.sta_hm, C.sta_hm), 'NULL') AS STA_HM, 
NVL(NVL(B.end_hm, C.end_hm),'NULL') AS END_HM, NVL(A.appl_type, 'NULL') AS TYPE, a.appl_id AS APPL_ID, 
NVL(NVL(b.del_yn, c.del_yn), 'NULL') AS DEL_YN, NVL(a.BF_APPL_ID, 'NULL') AS BF_APPL_ID, a.appl_txt as APPL_TXT
from ehr2011060.sy7010 A
left join (select appl_id, ymd, sta_hm, end_hm, del_yn
from ehr2011060.tam2215) B
on a.appl_id = b.appl_id
left join(select ymd, attend_cd, sta_hm, end_hm, appl_id, del_yn
from ehr2011060.tam5450) C
on a.appl_id = c.appl_id
where a.appl_stat_cd = '900' and (((a.appl_type='1002' or a.appl_type='1004' or a.appl_type='1008' or a.appl_type='1010') 
and NVL(B.ymd, C.ymd)=(SELECT TO_CHAR(SYSDATE-1, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)) 
or a.appl_type='1044' and substr(a.appl_txt,5,10)=(SELECT TO_CHAR(SYSDATE-1, 'YYYY.MM.DD')AS YYYYMMDD FROM DUAL))
"""
OracleCursor.execute(oracleSql)
origin_table=pd.DataFrame()

####### origin table에 신청 기록 받아오기 #######
for line in OracleCursor:
    data={'EMP_ID':line[0], 'APPR_YMD':line[1], 'YMD':line[2], 'STA_HM':line[3], 'END_HM':line[4], 'TYPE':line[5], 
        'APPL_ID':line[6], 'DEL_YN':line[7], 'BF_APPL_ID':line[8], 'APPL_TXT':line[9]}
    origin_table=origin_table.append(data,ignore_index=True)

####### origin table 취소기록 적용하여 정리하기 #######
bf_appl_id_list=[origin_table['BF_APPL_ID'].unique()]
origin_table = origin_table.drop(index=origin_table.loc[origin_table.APPL_ID.isin(bf_appl_id_list)].index)
#1. bf_appl_list와 매칭되는 행 삭제
origin_table = origin_table.drop(index=origin_table.loc[origin_table.DEL_YN == 'Y'].index)
#2. DEL_YN이 Y인 행 삭제
origin_table = origin_table.drop(index=origin_table.loc[origin_table.BF_APPL_ID != 'NULL'].index)
#3. bf_appl_id가 NULL이 아닌 행 삭제
origin_table = origin_table.drop(index=origin_table.loc[(origin_table.TYPE=='1010') & (origin_table.YMD == 'NULL')].index)


origin_table['TIME']=origin_table['STA_HM']+'~'+origin_table['END_HM']
origin_table.drop(['DEL_YN', 'BF_APPL_ID'], axis = 'columns', inplace= True)
# origin_table.drop(origin_table[origin_table['TIME']=='NULL~NULL'].index, inplace=True)

origin_table.loc[origin_table.TYPE =='1044','YMD']=datetime.now().strftime('%Y%m%d')
origin_table.loc[origin_table.TIME =='NULL~NULL','TIME']='0000~0000'

cur.execute('SELECT emp_id FROM connect.hr_info')
column=['YMD', 'EMP_ID', 'DAYOFF1_TIME','DAYOFF1_ID','DAYOFF2_TIME','DAYOFF2_ID','DAYOFF3_TIME','DAYOFF3_ID','DAYOFF4_TIME','DAYOFF4_ID',
'OVER1_TIME','OVER1_ID','OVER2_TIME','OVER2_ID','OVER3_TIME','OVER3_ID','OVER4_TIME','OVER4_ID','BUSI_TRIP1_TIME','BUSI_TRIP1_ID',
'BUSI_TRIP2_TIME','BUSI_TRIP2_ID','BUSI_TRIP3_TIME','BUSI_TRIP3_ID','BUSI_TRIP4_TIME','BUSI_TRIP4_ID',
'HOME_ID','ETC_INFO','ETC_ID']

merge_table = pd.DataFrame(columns=column)

for emp_id in cur: #사번정보 로드
    data={
        'EMP_ID': emp_id[0]
    }
    merge_table=merge_table.append(data, ignore_index=True)

merge_table = pd.merge(merge_table, origin_table, how='left', left_on='EMP_ID', right_on='EMP_ID')
merge_table.drop(['STA_HM', 'END_HM'], axis = 'columns', inplace= True)

def calculate():
    if merge_table.loc[line]['TYPE']=='1008': # 초과근무
        merge_table.loc[line]['OVER'+str(over)+'_TIME']=merge_table.loc[line]['TIME']
        merge_table.loc[line]['OVER'+str(over)+'_ID']=merge_table.loc[line]['APPL_ID']

    elif merge_table.loc[line]['TYPE']=='1002': #연차
        merge_table.loc[line]['DAYOFF1'+str(dayoff)+'_TIME']=merge_table.loc[line]['TIME']
        merge_table.loc[line]['DAYOFF1'+str(dayoff)+'_ID']=merge_table.loc[line]['APPL_ID']

    elif merge_table.loc[line]['TYPE']=='1010': #출장
        merge_table.loc[line]['BUSI_TRIP'+str(busi_trip)+'_TIME']=merge_table.loc[line]['TIME']
        merge_table.loc[line]['BUSI_TRIP'+str(busi_trip)+'_ID']=merge_table.loc[line]['APPL_ID']
    
    elif merge_table.loc[line]['TYPE']=='1044': #재택 
        merge_table.loc[line]['HOME_ID']=merge_table.loc[line]['APPL_ID']

    elif merge_table.loc[line]['TYPE']=='1004': #기타휴가 
        merge_table.loc[line]['ETC_INFO']=merge_table.loc[line]['APPL_TXT']
        merge_table.loc[line]['ETC_ID']=merge_table.loc[line]['APPL_ID']

over=1
dayoff=1
busi_trip=1

for line in range(len(merge_table)):
    if line==0:
        calculate()

    elif merge_table.loc[line]['EMP_ID']==merge_table.loc[line-1]['EMP_ID']:
        if merge_table.loc[line]['TYPE']=='1008': # 초과근무
            if merge_table.loc[line-1]['TYPE']=='1008':
                over +=1
                merge_table.loc[line]['OVER'+str(over)+'_TIME']=merge_table.loc[line]['TIME']
                merge_table.loc[line]['OVER'+str(over)+'_ID']=merge_table.loc[line]['APPL_ID']
            else:
                merge_table.loc[line]['OVER'+str(over)+'_TIME']=merge_table.loc[line]['TIME']
                merge_table.loc[line]['OVER'+str(over)+'_ID']=merge_table.loc[line]['APPL_ID']
                over +=1

        elif merge_table.loc[line]['TYPE']=='1002': #연차
            if merge_table.loc[line-1]['TYPE']=='1002':
                dayoff +=1
                merge_table.loc[line]['DAYOFF1'+str(dayoff)+'_TIME']=merge_table.loc[line]['TIME']
                merge_table.loc[line]['DAYOFF1'+str(dayoff)+'_ID']=merge_table.loc[line]['APPL_ID']
            else:
                merge_table.loc[line]['DAYOFF1'+str(dayoff)+'_TIME']=merge_table.loc[line]['TIME']
                merge_table.loc[line]['DAYOFF1'+str(dayoff)+'_ID']=merge_table.loc[line]['APPL_ID']
                dayoff +=1

        elif merge_table.loc[line]['TYPE']=='1010': #출장
            if merge_table.loc[line-1]['TYPE']=='1010':
                busi_trip +=1
                merge_table.loc[line]['BUSI_TRIP'+str(busi_trip)+'_TIME']=merge_table.loc[line]['TIME']
                merge_table.loc[line]['BUSI_TRIP'+str(busi_trip)+'_ID']=merge_table.loc[line]['APPL_ID']
            else:
                merge_table.loc[line]['BUSI_TRIP'+str(busi_trip)+'_TIME']=merge_table.loc[line]['TIME']
                merge_table.loc[line]['BUSI_TRIP'+str(busi_trip)+'_ID']=merge_table.loc[line]['APPL_ID']
                busi_trip +=1
        
        elif merge_table.loc[line]['TYPE']=='1044': #재택 
            merge_table.loc[line]['HOME_ID']=merge_table.loc[line]['APPL_ID']

        elif merge_table.loc[line]['TYPE']=='1004': #기타휴가 
            merge_table.loc[line]['ETC_INFO']=merge_table.loc[line]['APPL_TXT']
            merge_table.loc[line]['ETC_ID']=merge_table.loc[line]['APPL_ID']

    else:
        calculate()
        over=1
        dayoff=1
        busi_trip=1


merge_table.drop(['APPR_YMD', 'YMD_x', 'TYPE', 'APPL_ID', 'APPL_TXT', 'TIME'], axis = 'columns', inplace= True)

# merge_table['YMD_y']=datetime.now().strftime('%Y%m%d')

merge_table.fillna('None', inplace=True)
print(merge_table.head(30))

for line in range(len(merge_table)):
    if line==0:
        pass
    elif merge_table.loc[line]['EMP_ID']==merge_table.loc[line-1]['EMP_ID']:
        for i in range(29):
            if merge_table.loc[line-1][i]== 'None':
                merge_table.loc[line-1][i]=merge_table.loc[line][i]
                # merge_table.drop([line], inplace=True)
                # merge_table.reset_index()
                # i-=1
    else:
        pass


print(merge_table)

for i in range(len(merge_table)):
    sql="INSERT INTO `EHR_CAL` (`NUM`, `YMD`, `EMP_ID`,`DAYOFF1_TIME`,`DAYOFF1_ID`,`DAYOFF2_TIME`,`DAYOFF2_ID`,`DAYOFF3_TIME`,`DAYOFF3_ID`,`DAYOFF4_TIME`,`DAYOFF4_ID`,`OVER1_TIME`,`OVER1_ID`,`OVER2_TIME`,`OVER2_ID`,`OVER3_TIME`,`OVER3_ID`,`OVER4_TIME`,`OVER4_ID`,`BUSI_TRIP1_TIME`,`BUSI_TRIP1_ID`,`BUSI_TRIP2_TIME`,`BUSI_TRIP2_ID`,`BUSI_TRIP3_TIME`,`BUSI_TRIP3_ID`,`BUSI_TRIP4_TIME`,`BUSI_TRIP4_ID`,`HOME_ID`,`ETC_INFO`,`ETC_ID`) VALUES ("+"'"+str(i)+"'"+","+"'"+str(merge_table.loc[i][28])+"'"+","+"'"+str(merge_table.loc[i][0])+"'"+","+"'"+str(merge_table.loc[i][1])+"'"+","+"'"+str(merge_table.loc[i][2])+"'"+","+"'"+str(merge_table.loc[i][3])+"'"+","+"'"+str(merge_table.loc[i][4])+"'"+","+"'"+str(merge_table.loc[i][5])+"'"+","+"'"+str(merge_table.loc[i][6])+"'"+","+"'"+str(merge_table.loc[i][7])+"'"+","+"'"+str(merge_table.loc[i][8])+"'"+","+"'"+str(merge_table.loc[i][9])+"'"+","+"'"+str(merge_table.loc[i][10])+"'"+","+"'"+str(merge_table.loc[i][11])+"'"+","+"'"+str(merge_table.loc[i][12])+"'"+","+"'"+str(merge_table.loc[i][13])+"'"+","+"'"+str(merge_table.loc[i][14])+"'"+","+"'"+str(merge_table.loc[i][15])+"'"+","+"'"+str(merge_table.loc[i][16])+"'"+","+"'"+str(merge_table.loc[i][17])+"'"+","+"'"+str(merge_table.loc[i][18])+"'"+","+"'"+str(merge_table.loc[i][19])+"'"+","+"'"+str(merge_table.loc[i][20])+"'"+","+"'"+str(merge_table.loc[i][21])+"'"+","+"'"+str(merge_table.loc[i][22])+"'"+","+"'"+str(merge_table.loc[i][23])+"'"+","+"'"+str(merge_table.loc[i][24])+"'"+","+"'"+str(merge_table.loc[i][25])+"'"+","+"'"+str(merge_table.loc[i][26])+"'"+","+"'"+str(merge_table.loc[i][27])+"'"+")"
    cur.execute(sql)    

        # sql =f"""
        # INSERT INTO `EHR_CAL` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """
        # cur.execute(sql,(merge_table.loc[i]))
        # print(merge_table.loc[i])

conn.commit()      
conn.close()           


