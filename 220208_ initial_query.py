import cx_Oracle         # 오라클과 연동하는 패키지
import os
import pymysql             # mariadb와 연동하는 패키지
import pandas as pd
import sys, traceback
from datetime import datetime
from datetime import timedelta
from login_info import cx_Oracle_info as cxinfo, pymysql_info as mysqlinfo
for days_offset in range(1, 72):
    try: 
        LOCATION = "..\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3"         # 오라클 연동하는 프로그램의 위치 필요.
        os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
        OracleConnect = cx_Oracle.connect(cxinfo['id'], cxinfo['pw'], cxinfo['host'])       # 오라클 연동 정보입력
        OracleCursor = OracleConnect.cursor()  #오라클 sql문 쓰기 위한 커서

        conn=pymysql.connect(host=mysqlinfo['host'], user=mysqlinfo['user'], password=mysqlinfo['password'], 
                            db=mysqlinfo['db'], charset=mysqlinfo['charset'])       # mariadb 연동 정보입력
        cur=conn.cursor() #pymysql 커서

        # days_offset=int(input('몇일 전 데이터를 가져올까요?')) # (days_offset)일 전 데이터 가져오기
        now=datetime.now()
        that_moment=(now-timedelta(days=days_offset)).strftime('%Y%m%d')

        print(that_moment)

        oracleSql = f"""
        select NVL(A.trg_emp_id, 'NULL') AS EMP_ID, A.appr_ymd, NVL(NVL(B.ymd, C.ymd), 'NULL') AS YMD, NVL(NVL(B.sta_hm, C.sta_hm), 'NULL') AS STA_HM, 
        NVL(NVL(B.end_hm, C.end_hm),'NULL') AS END_HM, NVL(A.appl_type, 'NULL') AS TYPE, a.appl_id AS APPL_ID, 
        NVL(NVL(b.del_yn, c.del_yn), 'NULL') AS DEL_YN, NVL(a.BF_APPL_ID, 'NULL') AS BF_APPL_ID, a.appl_txt as APPL_TXT, NVL(B.reward_type, 'NULL') AS REWARD_TYPE
        from ehr2011060.sy7010 A
        left join (select appl_id, ymd, sta_hm, end_hm, del_yn, reward_type
        from ehr2011060.tam2215) B
        on a.appl_id = b.appl_id
        left join(select ymd, attend_cd, sta_hm, end_hm, appl_id, del_yn
        from ehr2011060.tam5450) C
        on a.appl_id = c.appl_id
        where a.appl_stat_cd = '900' and (( (a.appl_type='1002' or a.appl_type='1004' or a.appl_type='1008' or a.appl_type='1010') 
        and NVL(B.ymd, C.ymd)=(SELECT TO_CHAR(SYSDATE-{days_offset}, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)) 
        or a.appl_type='1044' and substr(a.appl_txt,5,10)=(SELECT TO_CHAR(SYSDATE-{days_offset}, 'YYYY.MM.DD')AS YYYYMMDD FROM DUAL))
        """
        OracleCursor.execute(oracleSql)
        origin_table=pd.DataFrame()

        for line in OracleCursor:
            data={'EMP_ID':line[0], 'APPR_YMD':line[1], 'YMD':line[2], 'STA_HM':line[3], 'END_HM':line[4], 'TYPE':line[5], 
                'APPL_ID':line[6], 'DEL_YN':line[7], 'BF_APPL_ID':line[8], 'APPL_TXT':line[9], 'REWARD_TYPE':line[10]}
            origin_table=origin_table.append(data,ignore_index=True)

        origin_table = origin_table.drop(index=origin_table.loc[origin_table.DEL_YN == 'Y'].index)
        #2. DEL_YN이 Y인 행 삭제
        origin_table = origin_table.drop(index=origin_table.loc[origin_table.BF_APPL_ID != 'NULL'].index)
        #3. bf_appl_id가 NULL이 아닌 행 삭제
        origin_table = origin_table.drop(index=origin_table.loc[(origin_table.TYPE=='1010') & (origin_table.YMD == 'NULL')].index)

        origin_table['TIME']=origin_table['STA_HM']+'~'+origin_table['END_HM']
        #STA_HM과 END_HM concat

        origin_table.drop(['DEL_YN', 'BF_APPL_ID'], axis = 'columns', inplace= True)

        origin_table.loc[origin_table.TYPE =='1044','YMD']=datetime.now().strftime('%Y%m%d')
        origin_table.loc[origin_table.TIME =='NULL~NULL','TIME']='0000~0000'

        # pd.set_option('display.max_row', 2000) # 최대 출력 행 설정

        column=['YMD','EMP_ID','NAME','ORG_NM','SHIFT_CD','WORK_TYPE','PLAN1','PLAN2', 'INOUT', 'FIX1','FIX2','DAYOFF1_TIME','DAYOFF1_ID','DAYOFF2_TIME','DAYOFF2_ID','DAYOFF3_TIME','DAYOFF3_ID','DAYOFF4_TIME','DAYOFF4_ID',
        'OVER1_TIME','OVER1_ID','OVER2_TIME','OVER2_ID','OVER3_TIME','OVER3_ID','OVER4_TIME','OVER4_ID','BUSI_TRIP1_TIME','BUSI_TRIP1_ID',
        'BUSI_TRIP2_TIME','BUSI_TRIP2_ID','BUSI_TRIP3_TIME','BUSI_TRIP3_ID','BUSI_TRIP4_TIME','BUSI_TRIP4_ID',
        'HOME_ID','ETC_INFO','ETC_ID','REWARD_TIME','REWARD_ID','CAL_OVERTIME','CAL_MEAL']

        cur.execute('SELECT emp_id, emp_nm, org_nm FROM connect.hr_info')

        merge_table = pd.DataFrame(columns=column)

        for emp_id in cur:#사번정보 로드
            data={
                'EMP_ID': emp_id[0],
                'NAME' : emp_id[1],
                'ORG_NM': emp_id[2],
                'YMD': that_moment
            }
            merge_table=merge_table.append(data, ignore_index=True)

        emp_id=merge_table['EMP_ID']

        merge_table.fillna('None', inplace=True) #NaN값 None으로 채우기

        temp_time='' #시작~끝 문자열 저장
        dayoff_string_list=['DAYOFF1_TIME','DAYOFF2_TIME','DAYOFF3_TIME','DAYOFF4_TIME']
        dayoff_string_id_list=['DAYOFF1_ID','DAYOFF2_ID','DAYOFF3_ID','DAYOFF4_ID']
        overtime_string_list=['OVER1_TIME','OVER2_TIME','OVER3_TIME','OVER4_TIME']
        overtime_string_id_list=['OVER1_ID','OVER2_ID','OVER3_ID','OVER4_ID']
        busitrip_string_list=['BUSI_TRIP1_TIME','BUSI_TRIP2_TIME','BUSI_TRIP3_TIME','BUSI_TRIP4_TIME']
        busitrip_string_id_list=['BUSI_TRIP1_ID','BUSI_TRIP2_ID','BUSI_TRIP3_ID','BUSI_TRIP4_ID']

        origin_table.reset_index(inplace=True, drop=False)

        insert_flag=0

        # 연차 / 초과근무 / 출장
        for idx in range(len(origin_table)):
            rows_origin=origin_table.loc[idx] #origin table 행
            cond_emp_id=merge_table['EMP_ID']==rows_origin['EMP_ID']
            merge_index=merge_table.loc[cond_emp_id,'EMP_ID'].keys()[0] #merge table과 사번 일치하는 행 인덱스넘버

            insert_flag=0 # 값이 삽입되었다는 것을 알리는 플래그. 1이 되면 string list 탐색을 중지하고 다음 origin table튜플을 탐색
            
            if rows_origin['TYPE']=='1008': #초과근무
                if rows_origin['STA_HM']=='NULL': # 널값 들어 있을 때
                    continue
                temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
                
                if rows_origin['REWARD_TYPE']!='NULL': # reward type이 정의되어 있을 때(주말)
                    
                    merge_table.at[merge_index,'REWARD_TIME']=temp_time
                    merge_table.at[merge_index,'REWARD_ID']=rows_origin['REWARD_TYPE']
                    continue
                
                for isvalue in overtime_string_list: #merge table 슬롯 하나씩 채우기
                    if insert_flag==1:
                        break
                    
                    if merge_table.loc[merge_index][isvalue]=='None':
                        insert_flag=1
                        merge_table.at[merge_index,isvalue]=temp_time
                        merge_table.at[merge_index,overtime_string_id_list[overtime_string_list.index(isvalue)]]=rows_origin['APPL_ID']

            elif rows_origin['TYPE']=='1002': #연차
                if rows_origin['STA_HM']=='NULL':
                    continue
                temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
                for isvalue in dayoff_string_list:
                    if insert_flag==1:
                        break
                    
                    if merge_table.loc[merge_index][isvalue]=='None':
                        insert_flag=1
                        merge_table.at[merge_index,isvalue]=temp_time
                        merge_table.at[merge_index,dayoff_string_id_list[dayoff_string_list.index(isvalue)]]=rows_origin['APPL_ID']
                        
            elif rows_origin['TYPE']=='1010': #출장
                temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
                for isvalue in busitrip_string_list:
                    if insert_flag==1:
                        break
                    
                    if merge_table.loc[merge_index][isvalue]=='None':
                        insert_flag=1
                        merge_table.at[merge_index,isvalue]=temp_time
                        merge_table.at[merge_index,busitrip_string_id_list[busitrip_string_list.index(isvalue)]]=rows_origin['APPL_ID']                

            elif rows_origin['TYPE']=='1044': #재택
                merge_table.at[merge_index, 'HOME_ID']=rows_origin['APPL_ID']
                
            elif rows_origin['TYPE']=='1004': #기타휴가 
                merge_table.at[merge_index, 'ETC_INFO']=rows_origin['APPL_TXT']
                merge_table.at[merge_index, 'ETC_ID']=rows_origin['APPL_ID']

            #근무유형 삽입하기
        OracleCursor.execute(f"SELECT EMP_ID,SHIFT_CD,WORK_TYPE FROM EHR2011060.TAM5400_V WHERE YMD =(SELECT TO_CHAR(SYSDATE-{days_offset}, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)")
        insert_table=pd.DataFrame() #근무유형 데이터프레임

        for line in OracleCursor:
            data={'EMP_ID':line[0], 'SHIFT_CD':line[1], 'WORK_TYPE':line[2]}
            insert_table=insert_table.append(data,ignore_index=True)
            
        for idx in range(len(insert_table)): #근무유형 삽입
            rows_insert=insert_table.loc[idx] #insert table 행
            cond_emp_id=merge_table['EMP_ID']==rows_insert['EMP_ID']
            comp_index=merge_table.loc[cond_emp_id,'EMP_ID'].keys()[0] #merge table과 사번 일치하는 행 인덱스넘버

            merge_table.loc[comp_index]['SHIFT_CD']=insert_table.loc[idx]['SHIFT_CD']
            merge_table.loc[comp_index]['WORK_TYPE']=insert_table.loc[idx]['WORK_TYPE']

        std_start = '0900'
        std_end = '1800'

            # 초과근무 적용하여 계획시간 만들기
        for i in range(len(merge_table)):
            overtime = ''   
            if merge_table.loc[i]['SHIFT_CD'] == '0010':
                if merge_table.loc[i]['OVER1_TIME'] != 'None':    #초과근무일때
                    over_start, over_end = merge_table.loc[i]['OVER1_TIME'].split('~')
                    if over_start < '0700':    #기존시간보다 작을때-> 초과근무 신청
                        overtime = over_start
                    else:
                        overtime = '0700'
                    overtime = overtime + '~'
                    if over_end > '1600':   # 기존시간보다 클때-> 초과근무 신청
                        overtime = overtime + over_end
                    else:
                        overtime = overtime + '1600'   
                    merge_table.loc[i]['PLAN1'] = overtime
                    merge_table.loc[i]['PLAN2'] = overtime
            elif merge_table.loc[i]['SHIFT_CD'] == '0020':
                if merge_table.loc[i]['OVER1_TIME'] != 'None':    #초과근무일때
                    over_start, over_end = merge_table.loc[i]['OVER1_TIME'].split('~')
                    if over_start < '0800':    #기존시간보다 작을때-> 초과근무 신청
                        overtime = over_start
                    else:
                        overtime = '0800'
                    overtime = overtime + '~'
                    if over_end > '1700':   # 기존시간보다 클때-> 초과근무 신청
                        overtime = overtime + over_end
                    else:
                        overtime = overtime + '1700'   
                    merge_table.loc[i]['PLAN1'] = overtime
                    merge_table.loc[i]['PLAN2'] = overtime
            elif merge_table.loc[i]['SHIFT_CD'] == '0040':
                if merge_table.loc[i]['OVER1_TIME'] != 'None':    #초과근무일때
                    over_start, over_end = merge_table.loc[i]['OVER1_TIME'].split('~')
                    if over_start < '1000':    #기존시간보다 작을때-> 초과근무 신청
                        overtime = over_start
                    else:
                        overtime = '1000'
                    overtime = overtime + '~'
                    if over_end > '1900':   # 기존시간보다 클때-> 초과근무 신청
                        overtime = overtime + over_end
                    else:
                        overtime = overtime + '1900'   
                    merge_table.loc[i]['PLAN1'] = overtime
                    merge_table.loc[i]['PLAN2'] = overtime
            elif merge_table.loc[i]['SHIFT_CD'] == '0440':
                if merge_table.loc[i]['OVER1_TIME'] != 'None':    #초과근무일때
                    over_start, over_end = merge_table.loc[i]['OVER1_TIME'].split('~')
                    if over_start < '0800':    #기존시간보다 작을때-> 초과근무 신청
                        overtime = over_start
                    else:
                        overtime = '0800'
                    overtime = overtime + '~'
                    if over_end > '1500':   # 기존시간보다 클때-> 초과근무 신청
                        overtime = overtime + over_end
                    else:
                        overtime = overtime + '1500'   
                    merge_table.loc[i]['PLAN1'] = overtime
                    merge_table.loc[i]['PLAN2'] = overtime
            elif merge_table.loc[i]['SHIFT_CD'] == '0170':
                if merge_table.loc[i]['OVER1_TIME'] != 'None':    #초과근무일때
                    over_start, over_end = merge_table.loc[i]['OVER1_TIME'].split('~')
                    if over_start < '1000':    #기존시간보다 작을때-> 초과근무 신청
                        overtime = over_start
                    else:
                        overtime = '1000'
                    overtime = overtime + '~'
                    if over_end > '1700':   # 기존시간보다 클때-> 초과근무 신청
                        overtime = overtime + over_end
                    else:
                        overtime = overtime + '1700'   
                    merge_table.loc[i]['PLAN1'] = overtime
                    merge_table.loc[i]['PLAN2'] = overtime
            else:
                if merge_table.loc[i]['OVER1_TIME'] != 'None':    #초과근무일때
                    over_start, over_end = merge_table.loc[i]['OVER1_TIME'].split('~')
                    if over_start < std_start:    #기존시간보다 작을때-> 초과근무 신청
                        overtime = over_start
                    else:
                        overtime = std_start
                    overtime = overtime + '~'
                    if over_end > std_end:   # 기존시간보다 클때-> 초과근무 신청
                        overtime = overtime + over_end
                    else:
                        overtime = overtime + std_end   
                    merge_table.loc[i]['PLAN1'] = overtime
                    merge_table.loc[i]['PLAN2'] = overtime

            # 기록기 시간 만들기
        for i in range(len(emp_id)):
            inout = ''
            tmp_end = ''
            cur.execute(f"SELECT WORK_INFO_CLOCK FROM connect.at_att_inout AS T WHERE " + emp_id[i] + f" = T.EMP_CODE AND {that_moment}= T.WORK_DATE AND T.WORK_CD = 'IN' ORDER BY T.WORK_INFO_CLOCK LIMIT 1") 
            for line in cur:   # 들어올때 시간 삽입
                inout = line[0]
            cur.execute(f"SELECT WORK_INFO_CLOCK FROM connect.at_att_inout AS T WHERE " + emp_id[i] + f" = T.EMP_CODE AND {that_moment}= T.WORK_DATE AND T.WORK_CD = 'OUT' ORDER BY T.WORK_INFO_CLOCK DESC LIMIT 1") 
            inout = inout + '~'
            for line in cur:   # 나갈때 시간 삽입
                tmp_end = line[0]
                inout = inout + tmp_end
            merge_table.loc[i]['INOUT'] = inout

        def pre_set(_str, _end): # 초과근무 없을때, 전일연차, 전일 출장
            if merge_table.loc[i]['OVER1_TIME'] == 'None':
                merge_table.loc[i]['PLAN1'] = _str +'~'+ _end
                merge_table.loc[i]['PLAN2'] = _str +'~'+ _end
            if merge_table.loc[i]['DAYOFF1_TIME'] == _str +'~'+ _end:
                merge_table.loc[i]['PLAN2'] = '전일연차'
            if merge_table.loc[i]['BUSI_TRIP1_TIME'] == _str +'~'+ _end:
                merge_table.loc[i]['PLAN2'] = _str +'~'+ _end

        def plan_not_inout(_str, _end): # 출퇴근버튼 안찍었을 때 연차, 출장 반영
            if merge_table.loc[i]['DAYOFF1_TIME'] == _str +'~'+ _end:
                merge_table.loc[i]['PLAN2'] = '전일연차'
            elif merge_table.loc[i]['DAYOFF1_TIME'] != 'None':
                if merge_table.loc[i]['PLAN2'][-4:] == merge_table.loc[i]['DAYOFF1_TIME'][-4:]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['PLAN2'][:4]+ '~' +merge_table.loc[i]['DAYOFF1_TIME'][:4]
                if merge_table.loc[i]['PLAN2'][:4] == merge_table.loc[i]['DAYOFF1_TIME'][:4]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['DAYOFF1_TIME'][-4:] +'~'+ merge_table.loc[i]['PLAN2'][-4:]
            if merge_table.loc[i]['BUSI_TRIP1_TIME'] == _str +'~'+ _end:
                merge_table.loc[i]['PLAN2'] = _str +'~'+ _end
            elif merge_table.loc[i]['BUSI_TRIP1_TIME'] != 'None':
                if merge_table.loc[i]['PLAN2'][-4:] == merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['PLAN2'][:4]+'~'+ merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]
                if merge_table.loc[i]['PLAN2'][:4] == merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:] +'~'+ merge_table.loc[i]['PLAN2'][-4:]

        def plan_not_in(): # 퇴근 버튼만 찍었을 때 연차, 출장 반영
            if merge_table.loc[i]['DAYOFF1_TIME'] != 'None':
                if merge_table.loc[i]['PLAN2'][-4:] == merge_table.loc[i]['DAYOFF1_TIME'][-4:]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['PLAN2'][:4]+ '~'+ merge_table.loc[i]['DAYOFF1_TIME'][:4]
                if merge_table.loc[i]['PLAN2'][:4] == merge_table.loc[i]['DAYOFF1_TIME'][:4]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['DAYOFF1_TIME'][-4:] +'~'+ merge_table.loc[i]['PLAN2'][-4:]
            if merge_table.loc[i]['BUSI_TRIP1_TIME'] != 'None':
                if merge_table.loc[i]['PLAN2'][-4:] == merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:]:
                    merge_table.loc[i]['PLAN2'] == merge_table.loc[i]['PLAN2'][:4]+ '~'+ merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]
                if merge_table.loc[i]['PLAN2'][:4] == merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:] +'~'+ merge_table.loc[i]['PLAN2'][-4:]

        def plan_all_notout(): # 출근 버튼만 찍었을 때 연차, 출장 반영 / 모두 찍었을 때 반영
            if merge_table.loc[i]['DAYOFF1_TIME'] != 'None':
                if merge_table.loc[i]['PLAN2'][:4] == merge_table.loc[i]['DAYOFF1_TIME'][:4]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['DAYOFF1_TIME'][-4:] +'~'+ merge_table.loc[i]['PLAN2'][-4:]
                if merge_table.loc[i]['PLAN2'][-4:] == merge_table.loc[i]['DAYOFF1_TIME'][-4:]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['PLAN2'][:4] +'~'+ merge_table.loc[i]['DAYOFF1_TIME'][:4]
            if merge_table.loc[i]['BUSI_TRIP1_TIME'] != 'None':
                if merge_table.loc[i]['PLAN2'][:4] == merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:] +'~'+ merge_table.loc[i]['PLAN2'][-4:]
                if merge_table.loc[i]['PLAN2'][:4] == merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]:
                    merge_table.loc[i]['PLAN2'] = merge_table.loc[i]['PLAN2'][:4] +'~'+ merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]

        def common(_str, _end): # 기본출퇴근
            if merge_table.loc[i]['INOUT'] == '~': #기록기 시간 없을 때
                plan_not_inout(_str,_end)
                merge_table.loc[i]['INOUT'] = _str+ '~' +_end
            elif merge_table.loc[i]['INOUT'][0] == '~': # 퇴근버튼만 찍었을 때
                merge_table.loc[i]['INOUT']= _str+ merge_table.loc[i]['INOUT']
                plan_not_in()
            elif merge_table.loc[i]['INOUT'][-1] == '~': # 출근버튼만 찍었을 때
                merge_table.loc[i]['INOUT'] += _end
                plan_all_notout()
            else: # 출퇴근 버튼 찍었을 때
                plan_all_notout()

        def fix_end(_end): # 퇴근버튼만 찍었을 때 확정시간
            fixtime = ''
            inout_end=merge_table.loc[i]['INOUT'][-4:]
            inout_end_hour = inout_end[:2]
            inout_end_min = inout_end[-2:]
            inout_end_cal = int(inout_end_hour)*60 + int(inout_end_min)
            if merge_table.loc[i]['PLAN1'][-4:] > _end: #퇴근이후 시간외 신청
                if inout_end_cal >= int(_end[:2])*60+30:
                    if merge_table.loc[i]['PLAN1'][-4:] < inout_end:
                        fixtime = fixtime + merge_table.loc[i]['PLAN1'][-4:]
                    else:
                        fixtime += inout_end                  
                else:
                    fixtime = fixtime + _end
            elif merge_table.loc[i]['PLAN1'][-4:] == _end: #퇴근이전 시간외 미신청
                if inout_end_cal >= int(_end[:2])*60:
                    fixtime += _end
                else:
                    fixtime += merge_table.loc[i]['INOUT'][-4:]
            merge_table.loc[i]['FIX1'] = '~'+ fixtime

        def fix_start(_start): # 출근버튼만 찍었을 때 확정시간
            fixtime = ''
            inout_start= merge_table.loc[i]['INOUT'][:4]
            inout_start_hour = inout_start[:2]
            inout_start_min = inout_start[-2:]
            inout_start_cal = int(inout_start_hour)*60 + int(inout_start_min)
            if merge_table.loc[i]['PLAN1'][:4] < _start: #출근이전 시간외 신청
                if int(_start[:2])*60-30 >= inout_start_cal: #30분 빼고 비교
                    fixtime = inout_start
                    if merge_table.loc[i]['PLAN1'][:4] > inout_start:
                        fixtime = merge_table.loc[i]['PLAN1'][:4]
                elif int(_start[:2])*60-30 <= inout_start_cal <= int(_start[:2])*60:
                    fixtime = _start
                else:
                    fixtime= merge_table.loc[i]['INOUT'][:4]
            elif merge_table.loc[i]['PLAN1'][:4] == _start: #출근이전 시간외 미신청
                if merge_table.loc[i]['INOUT'][:4] < _start:
                    fixtime = _start
                else:
                    fixtime = merge_table.loc[i]['INOUT'][:4]
            fixtime = fixtime + '~'
            merge_table.loc[i]['FIX1']=fixtime

        def fix_all(_str, _end):
            fixtime = ''
            inout_start, inout_end = merge_table.loc[i]['INOUT'].split('~')
            # 시간 비교를 위해 분단위로 고쳐주기
            inout_start_hour = inout_start[:2]
            inout_start_min = inout_start[-2:]
            inout_end_hour = inout_end[:2]
            inout_end_min = inout_end[-2:]
            inout_start_cal = int(inout_start_hour)*60 + int(inout_start_min)
            inout_end_cal = int(inout_end_hour)*60 + int(inout_end_min)
            #출근
            if merge_table.loc[i]['PLAN1'][:4] < _str: #출근이전 시간외 신청
                if int(_str[:2])*60-30 >= inout_start_cal: #30분 빼고 비교
                    fixtime = inout_start
                    if merge_table.loc[i]['PLAN1'][:4] > inout_start:
                        fixtime = merge_table.loc[i]['PLAN1'][:4]
                elif  int(_str[:2])*60-30 <= inout_start_cal <= int(_str[:2])*60:
                    fixtime = _str
                else:
                    fixtime= merge_table.loc[i]['INOUT'][:4]
            elif merge_table.loc[i]['PLAN1'][:4] == _str: #출근이전 시간외 미신청
                if merge_table.loc[i]['INOUT'][:4] < _str:
                    fixtime = _str
                else:
                    fixtime = merge_table.loc[i]['INOUT'][:4]
            fixtime = fixtime + '~'
            #퇴근
            if merge_table.loc[i]['PLAN1'][-4:] > _end: #퇴근이후 시간외 신청
                if inout_end_cal >= int(_end[:2])*60+30:
                    if merge_table.loc[i]['PLAN1'][-4:] < inout_end:
                        fixtime = fixtime + merge_table.loc[i]['PLAN1'][-4:]
                    else:
                        fixtime += inout_end                  
                else:
                    fixtime = fixtime + _end
            elif merge_table.loc[i]['PLAN1'][-4:] == _end: #퇴근이전 시간외 미신청
                if merge_table.loc[i]['INOUT'][-4:] > merge_table.loc[i]['PLAN1'][-4:]:
                    fixtime +=_end
                else:
                    fixtime += merge_table.loc[i]['INOUT'][-4:]
            merge_table.loc[i]['FIX1'] = fixtime

        for i in range(len(merge_table)):
            if merge_table.loc[i]['WORK_TYPE']=='None': # NULL 값 제외
                continue

            elif merge_table.loc[i]['WORK_TYPE']=='0060': # 휴일 근무 계산
                if merge_table.loc[i]['REWARD_ID'] == '100':
                    merge_table.loc[i]['PLAN1'] =  merge_table.loc[i]['REWARD_TIME']
                    merge_table.loc[i]['PLAN2'] =  merge_table.loc[i]['REWARD_TIME']
                    hol_str = merge_table.loc[i]['PLAN1'][:4]
                    hol_end = merge_table.loc[i]['PLAN1'][-4:]
                    if merge_table.loc[i]['INOUT'] == '~':
                        plan_not_inout(hol_str, hol_end)
                    elif merge_table.loc[i]['INOUT'][0] == '~':
                        plan_not_in()
                        fix_end(hol_end)
                    elif merge_table.loc[i]['INOUT'][-1] == '~':
                        plan_all_notout()
                        fix_start(hol_str)
                    else:
                        plan_all_notout()
                        fix_all(hol_str, hol_end)
                        # hol_str = merge_table.loc[i]['FIX1'][:4]
                        # hol_end = merge_table.loc[i]['FIX1'][-4:]
                        # hol_str_hr = hol_str[:2]
                        # hol_str_mn = hol_str[2:]
                        # hol_end_hr = hol_end[:2]
                        # hol_end_mn = hol_end[2:]
                        # cal_str = int(hol_str_hr)*60+int(hol_str_mn)
                        # cal_end = int(hol_end_hr)*60+int(hol_end_mn)
                        # if (cal_end-cal_str)>=240:
                        #     merge_table.loc[i]['FIX1'] = merge_table.loc[i]['PLAN1'][:4]+'~'+ str(int(merge_table.loc[i]['PLAN1'][:4])+400)
                    continue
                elif merge_table.loc[i]['REWARD_ID'] == '400':
                    merge_table.loc[i]['PLAN1'] =  merge_table.loc[i]['REWARD_TIME']
                    merge_table.loc[i]['PLAN2'] =  merge_table.loc[i]['REWARD_TIME']
                    continue
                else:
                    continue
            else:
                if merge_table.loc[i]['SHIFT_CD']=='0030': #기본출퇴근자
                    if merge_table.loc[i]['REWARD_ID'] =='400':
                        pre_set('0900', '1800')
                        if merge_table.loc[i]['PLAN1'] != merge_table.loc[i]['REWARD_TIME']:
                            merge_table.loc[i]['FIX1'] = 'ERROR'
                        else:
                            if merge_table.loc[i]['PLAN1'][:4]<merge_table.loc[i]['INOUT'][:4]:
                                merge_table.loc[i]['FIX1'] = 'None'
                            elif merge_table.loc[i]['PLAN1'][-4:]>merge_table.loc[i]['INOUT'][-4:]:
                                merge_table.loc[i]['FIX1'] = 'None'
                            else:
                                merge_table.loc[i]['FIX1'] = merge_table.loc[i]['REWARD_TIME']

                    else:
                        pre_set('0900', '1800')
                        common('0900', '1800')
                        if merge_table.loc[i]['WORK_TYPE']=='0030': #기본출퇴근
                            fixtime = ''
                            inout_start, inout_end = merge_table.loc[i]['INOUT'].split('~')
                            # 시간 비교를 위해 분단위로 고쳐주기
                            inout_start_hour = inout_start[:2]
                            inout_start_min = inout_start[-2:]
                            inout_end_hour = inout_end[:2]
                            inout_end_min = inout_end[-2:]
                            inout_start_cal = int(inout_start_hour)*60 + int(inout_start_min)
                            inout_end_cal = int(inout_end_hour)*60 + int(inout_end_min)

                            #출근
                            if merge_table.loc[i]['PLAN1'][:4] < '0900': #출근이전 시간외 신청
                                if 510 >= inout_start_cal: #30분 빼고 비교
                                    fixtime = inout_start
                                    if merge_table.loc[i]['PLAN1'][:4] > inout_start:
                                        fixtime = merge_table.loc[i]['PLAN1'][:4]
                                else:
                                    fixtime = '0900'
                            elif merge_table.loc[i]['PLAN1'][:4] == '0900': #출근이전 시간외 미신청
                                fixtime = '0900'
                            fixtime = fixtime + '~'

                            #퇴근
                            if merge_table.loc[i]['PLAN1'][-4:] > '1800': #퇴근이후 시간외 신청
                                if inout_end_cal >= 1110:
                                    if merge_table.loc[i]['PLAN1'][-4:] < inout_end:
                                        fixtime = fixtime + merge_table.loc[i]['PLAN1'][-4:]
                                    else:
                                        fixtime += inout_end                  
                                else:
                                    fixtime = fixtime + '1800'
                            elif merge_table.loc[i]['PLAN1'][-4:] == '1800': #퇴근이전 시간외 미신청
                                fixtime = fixtime + '1800'
                            merge_table.loc[i]['FIX1'] = fixtime

                        elif merge_table.loc[i]['WORK_TYPE']=='0290': #기본출퇴근자-재택
                            inout=''
                            inout_in = merge_table.loc[i]['INOUT'][:4]
                            inout_out = merge_table.loc[i]['INOUT'][5:]

                            if inout_in<='0900':
                                inout='0900~'
                            else:
                                inout=inout_in + '~'
                            if inout_out >='1800':
                                inout += '1800'
                            else:
                                inout += inout_out
                            merge_table.loc[i]['FIX1']= inout

                elif merge_table.loc[i]['SHIFT_CD']=='0010': #시차출퇴근(8-17),시차출퇴근(8-17)_재택
                    pre_set('0700', '1600')
                    if merge_table.loc[i]['INOUT'] == '~':
                        plan_not_inout('0700', '1600')
                    elif merge_table.loc[i]['INOUT'][0] == '~':
                        plan_not_in()
                        fix_end('1600')
                    elif merge_table.loc[i]['INOUT'][-1] == '~':
                        plan_all_notout()
                        fix_start('0700')
                    else:
                        plan_all_notout()
                        fix_all('0700', '1600')
                
                elif merge_table.loc[i]['SHIFT_CD']=='0020': #시차출퇴근(8-17),시차출퇴근(8-17)_재택
                    pre_set('0800', '1700')
                    if merge_table.loc[i]['INOUT'] == '~':
                        plan_not_inout('0800', '1700')
                    elif merge_table.loc[i]['INOUT'][0] == '~':
                        plan_not_in()
                        fix_end('1700')
                    elif merge_table.loc[i]['INOUT'][-1] == '~':
                        plan_all_notout()
                        fix_start('0800')
                    else:
                        plan_all_notout()
                        fix_all('0800', '1700')

                elif merge_table.loc[i]['SHIFT_CD']=='0040': #시차출퇴근(10-19), 시차출퇴근(10-19)_재택
                    pre_set('1000','1900')
                    if merge_table.loc[i]['INOUT'] == '~':
                        plan_not_inout('1000','1900')
                    elif merge_table.loc[i]['INOUT'][0] == '~':
                        plan_not_in()
                        fix_end('1900')
                    elif merge_table.loc[i]['INOUT'][-1] == '~':
                        plan_all_notout()
                        fix_start('1000')
                    else:
                        plan_all_notout()
                        fix_all('1000','1900')

                elif merge_table.loc[i]['SHIFT_CD']=='0440': #임신기 근로단축(8-15), 임신기 근로단축(8-15)_재택
                    pre_set('0800','1500')
                    if merge_table.loc[i]['INOUT'] == '~':
                        plan_not_inout('0800','1500')
                    elif merge_table.loc[i]['INOUT'][0] == '~':
                        plan_not_in()
                        fix_end('1500')
                    elif merge_table.loc[i]['INOUT'][-1] == '~':
                        plan_all_notout()
                        fix_start('0800')
                    else:
                        plan_all_notout()
                        fix_all('0800','1500')

                elif merge_table.loc[i]['SHIFT_CD']=='0170': #육아기 근로단축(10-17), 육아기 근로단축(10-17)_재택
                    pre_set('1000','1700')
                    if merge_table.loc[i]['INOUT'] == '~':
                        plan_not_inout('1000','1700')
                    elif merge_table.loc[i]['INOUT'][0] == '~':
                        plan_not_in()
                        fix_end('1700')
                    elif merge_table.loc[i]['INOUT'][-1] == '~':
                        plan_all_notout()
                        fix_start('1000')
                    else:
                        plan_all_notout()
                        fix_all('1000','1700')
            # 예외처리 전부 =>확정시간 반영
            err = 'ERROR'
            fix_in = merge_table.loc[i]['DAYOFF1_TIME'][-4:]
            fix_in2 = merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:]
            fix_out = merge_table.loc[i]['DAYOFF1_TIME'][:4]
            fix_out2 = merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]
            if merge_table.loc[i]['SHIFT_CD']=='0010' and merge_table.loc[i]['PLAN1'][:4]>'0700': #계획시간 잘못 반영 - 출근 계획 
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0010' and merge_table.loc[i]['PLAN1'][-4:]<'1600': #계획시간 잘못 반영 - 퇴근 계획
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0010' and '0630'<merge_table.loc[i]['PLAN1'][:4]<'0700': #계획시간 잘못 반영- 30분 미만
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0010' and '1600'<merge_table.loc[i]['PLAN1'][:4]<'1630': #계획시간 잘못 반영- 30분 미만
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0020' and merge_table.loc[i]['PLAN1'][:4]>'0800': 
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0020' and merge_table.loc[i]['PLAN1'][-4:]<'1700': 
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0020' and '0730'<merge_table.loc[i]['PLAN1'][:4]<'0800': 
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0020' and '1700'<merge_table.loc[i]['PLAN1'][:4]<'1730': 
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0030' and merge_table.loc[i]['PLAN1'][:4]>'0900': 
                merge_table.loc[i]['FIX1'] = err 
            if merge_table.loc[i]['SHIFT_CD']=='0030' and merge_table.loc[i]['PLAN1'][-4:]<'1800': 
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0030' and '0830'<merge_table.loc[i]['PLAN1'][:4]<'0900':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0030' and '1800'<merge_table.loc[i]['PLAN1'][:4]<'1830':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0040' and merge_table.loc[i]['PLAN1'][:4]>'1000':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0040' and merge_table.loc[i]['PLAN1'][-4:]<'1900':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0040' and '0930'<merge_table.loc[i]['PLAN1'][:4]<'1000':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0040' and '1900'<merge_table.loc[i]['PLAN1'][:4]<'1930':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0440' and merge_table.loc[i]['PLAN1'][:4]>'0800':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0440' and merge_table.loc[i]['PLAN1'][-4:]<'1500':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0440' and '0730'<merge_table.loc[i]['PLAN1'][:4]<'0800':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0440' and '1500'<merge_table.loc[i]['PLAN1'][:4]<'1530':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0170' and merge_table.loc[i]['PLAN1'][:4]>'1000':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0170' and merge_table.loc[i]['PLAN1'][-4:]<'1700':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0170' and '0930'<merge_table.loc[i]['PLAN1'][:4]<'1000':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['SHIFT_CD']=='0170' and '1700'<merge_table.loc[i]['PLAN1'][:4]<'1730':
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['PLAN1'][:4] >= merge_table.loc[i]['PLAN1'][-4:]: # 계획시간 잘못 반영 - 출퇴근 계획 같거나 반대일 경우
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['PLAN1'][:4] == merge_table.loc[i]['DAYOFF1_TIME'][:4]: # 출근시간 연차 포함일때
                if merge_table.loc[i]['DAYOFF1_TIME'][:4] <= merge_table.loc[i]['INOUT'][:4] <= merge_table.loc[i]['DAYOFF1_TIME'][-4:]: # 시간안에 온 경우
                    merge_table.loc[i]['FIX1'] = merge_table.loc[i]['PLAN1'][:4] + merge_table.loc[i]['FIX1'][4:]
            if merge_table.loc[i]['PLAN1'][-4:] == merge_table.loc[i]['DAYOFF1_TIME'][-4:]: # 퇴근시간 연차 포함일때
                if merge_table.loc[i]['DAYOFF1_TIME'][:4] <= merge_table.loc[i]['INOUT'][-4:] <= merge_table.loc[i]['DAYOFF1_TIME'][-4:]: # 시간안에 온 경우
                    merge_table.loc[i]['FIX1'] = merge_table.loc[i]['FIX1'][:4] + merge_table.loc[i]['PLAN1'][4:]
            if merge_table.loc[i]['PLAN1'][:4] == merge_table.loc[i]['BUSI_TRIP1_TIME'][:4]: # 출근시간 출장 포함일때
                if merge_table.loc[i]['BUSI_TRIP1_TIME'][:4] <= merge_table.loc[i]['INOUT'][:4] <= merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:]: # 시간안에 온 경우
                    merge_table.loc[i]['FIX1'] = merge_table.loc[i]['PLAN1'][:4] + merge_table.loc[i]['FIX1'][4:]
            if merge_table.loc[i]['PLAN1'][-4:] == merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:]: # 퇴근시간 출장 포함일때
                if merge_table.loc[i]['INOUT'][-1]== '~': # 퇴근 안 찍고 가신경우
                    merge_table.loc[i]['FIX1'] = merge_table.loc[i]['FIX1'][:4]+ merge_table.loc[i]['PLAN1'][4:]
                if merge_table.loc[i]['BUSI_TRIP1_TIME'][:4] <= merge_table.loc[i]['INOUT'][-4:] <= merge_table.loc[i]['BUSI_TRIP1_TIME'][-4:]: # 시간안에 온 경우
                    merge_table.loc[i]['FIX1'] = merge_table.loc[i]['FIX1'][:4] + merge_table.loc[i]['PLAN1'][4:]
            if merge_table.loc[i]['PLAN1'] == merge_table.loc[i]['DAYOFF1_TIME']: # 종일 연차
                merge_table.loc[i]['FIX1'] = merge_table.loc[i]['PLAN1']
            if merge_table.loc[i]['PLAN1'] == merge_table.loc[i]['BUSI_TRIP1_TIME']: # 종일 출장
                merge_table.loc[i]['FIX1'] = merge_table.loc[i]['PLAN1']
            if merge_table.loc[i]['DAYOFF1_TIME'][-4:] == '1200' or merge_table.loc[i]['DAYOFF1_TIME'][:4] == '1300': # 점심시간 제외
                merge_table.loc[i]['FIX1'] = err
            if  merge_table.loc[i]['ETC_ID'] != 'None': # 기타휴가
                merge_table.loc[i]['FIX1'] = '기타휴가'
            if merge_table.loc[i]['DAYOFF2_ID'] !='None' or merge_table.loc[i]['DAYOFF3_ID'] !='None' or merge_table.loc[i]['DAYOFF4_ID'] !='None': # 연차 2번 쓴 경우
                merge_table.loc[i]['FIX1'] = err
            if merge_table.loc[i]['BUSI_TRIP2_ID'] !='None' or merge_table.loc[i]['BUSI_TRIP3_ID'] !='None' or merge_table.loc[i]['BUSI_TRIP4_ID'] !='None': # 출장 2번 쓴 경우
                merge_table.loc[i]['FIX1'] = err
            if len(merge_table.loc[i]['FIX1']) == 9 and 'None' in merge_table.loc[i]['FIX1']: # 시간에 none 들어갈때
                merge_table.loc[i]['FIX1'] = err
            if len(merge_table.loc[i]['FIX1']) == 9 and 'ERRO' in merge_table.loc[i]['FIX1']: # 시간에 erro 들어갈때
                merge_table.loc[i]['FIX1'] = err
        

                
        #초과근무 시간 계산  
        # 주말 초과근무 계산 로직 추가할것 - 조권호
        for i in range(len(merge_table)):
            cal_overtime=0
            if merge_table.loc[i]['FIX1']=='None' or len(merge_table.loc[i]['FIX1'])!=9 or merge_table.loc[i]['FIX1']== 'ERROR' or merge_table.loc[i]['FIX1'] == '대휴' or merge_table.loc[i]['FIX1'] == '기타휴가':
                #FIX1이 None이거나 시작또는 끝이 비어있을때 (추후에 로직 개선)
                cal_overtime='0000'
            else: #정상적인 경우
                # cal_overtime=int(merge_table.loc[i]['FIX1'][-4:])-int(merge_table.loc[i]['FIX1'][:4])-900
                cal_overtime_start=timedelta(
                    minutes=int(merge_table.loc[i]['FIX1'][2:4]),
                    hours=int(merge_table.loc[i]['FIX1'][:2]),
                )
                cal_overtime_end=timedelta(
                    minutes=int(merge_table.loc[i]['FIX1'][-2:]),
                    hours=int(merge_table.loc[i]['FIX1'][-4:-2]),
                )
                if merge_table.loc[i]['WORK_TYPE']=='0060':
                    cal_overtime=str(cal_overtime_end-cal_overtime_start).zfill(8)
                    cal_overtime=cal_overtime[:2]+cal_overtime[3:5]
                    if cal_overtime[0]=='-': #음수일때
                        cal_overtime='0000'
                    elif int(cal_overtime)>=400: # 4시간 초과할때
                        cal_overtime='0400'
                    elif int(cal_overtime)<200:
                        cal_overtime='0000'
                else :
                    cal_overtime=str(cal_overtime_end-cal_overtime_start-timedelta(hours=9)).zfill(8)
                    cal_overtime=cal_overtime[:2]+cal_overtime[3:5]
                    if cal_overtime[0]=='-': #음수일때
                        cal_overtime='0000'
                    elif int(cal_overtime)>=400: # 4시간 초과할때
                        cal_overtime='0400'
                    elif int(cal_overtime)<30:
                        cal_overtime='0000'
            
            merge_table.loc[i]['CAL_OVERTIME']=cal_overtime

        #급량비 계산(임신기근로, 육아기근로 미반영)
        for i in range(len(merge_table)):
            merge_table.loc[i]['CAL_MEAL']='FALSE'
            work_time_range=''#근무유형 시간 범위
            
            if merge_table.loc[i]['CAL_OVERTIME']<'0100': # 초과근무 시간이 한시간 미만이면 급량비 산정 FALSE
                continue
            
            if merge_table.loc[i]['FIX1']=='None' or len(merge_table.loc[i]['FIX1'])!=9: #잘못된 정보 들어가있으면 급량비 산정 FALSE
                continue
            
            if merge_table.loc[i]['WORK_TYPE'] in ['0290','0280','0300']: # 재택근무이면 초과근무 0시간이므로 급량비 산정 FALSE
                continue
            
            if merge_table.loc[i]['WORK_TYPE']=='0060': #주말근무일 때 
                if merge_table.loc[i]['CAL_OVERTIME']>='0200': #2시간 이상이어야 TRUE
                    merge_table.loc[i]['CAL_MEAL']='TRUE'

            elif merge_table.loc[i]['SHIFT_CD']=='0030': # 09~18 근무자일 때
                # 시작시간이 08시 이하거나 끝시간이 19시 이후이면
                
                if merge_table.loc[i]['FIX1'][:4]<='0800' or merge_table.loc[i]['FIX1'][-4:]>='1900': 
                    merge_table.loc[i]['CAL_MEAL']='TRUE'
                    
            elif merge_table.loc[i]['SHIFT_CD']=='0040': # 10~19 근무자일 때
                # 시작시간이 08시 이하거나 끝시간이 20시 이후이면
                if merge_table.loc[i]['FIX1'][:4]<='0800' or merge_table.loc[i]['FIX1'][-4:]>='2000':
                    merge_table.loc[i]['CAL_MEAL']='TRUE'
            elif merge_table.loc[i]['SHIFT_CD']=='0020': # 08~17 근무자일 때
                # 시작시간이 07시 이하거나 끝시간이 19시 이후이면
                if merge_table.loc[i]['FIX1'][:4]<='0700' or merge_table.loc[i]['FIX1'][-4:]>='1900':
                    merge_table.loc[i]['CAL_MEAL']='TRUE'
            elif merge_table.loc[i]['SHIFT_CD']=='0010': # 07~16 근무자일 때
                # 시작시간이 06시 이하거나 끝시간이 19시 이후이면
                if merge_table.loc[i]['FIX1'][:4]<='0600' or merge_table.loc[i]['FIX1'][-4:]>='1900':
                    merge_table.loc[i]['CAL_MEAL']='TRUE'

        idx=merge_table[merge_table['SHIFT_CD']=='None'].index
        merge_table.drop(idx, inplace=True)
        merge_table=merge_table.reset_index(drop=True)
        parameters='%s,'*42

        # print('2015026 정보', merge_table[merge_table['EMP_ID']=='20150026'])
        # print(merge_table.head(40))

        for i in range(len(merge_table)):
            sql=f"INSERT INTO connect.ehr_cal values ({str(i+1)}, {parameters[:-1]})" #날짜별 NUM(사번연번) + 42개의 parameters
            cur.execute(sql, list(merge_table.loc[i]))
    except Exception as e:
        print(e)
        traceback.print_exc()
        sys.exit()
    finally:
        conn.commit()
        conn.close()
        OracleConnect.close()