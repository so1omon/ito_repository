# 인천관광공사 직원 근태 기록 생성기

2022.04.01 작성 및 설계 시작

cf) [] : DB 또는 dataframe 컬럼명

1. 전체 예외처리를 위한 try문 작성

2. DB connect 이후
 > 1) 특정 날짜
 > 2) 기간 설정
 > 날짜기간 지정하기 위한 input() form 작성
 > 반복문이 돌아가는 날짜를 today(또는 target_day)에 저장
 
3. origin_table 생성
 > 초기 신청 정보 가져오기
 > [appl_id] 또는 ymd가 null이면 유효하지 않은 신청 정보로 판단하여 삭제
 > [DEL_YN] == 'Y'(결재취소)인 정보 삭제
 > [appl_id] in [bf_appl_id]이면 삭제
 > [TYPE] == '1044'인 데이터의 YMD를 현재 날짜로 변경
 > [TIME] <= [STA_HM(hhmm)]~[END_HM(hhmm)]
 > [TIME] == "NULL NULL"인 데이터를 '0000~~0000'으로 변경
 > dataframe에 삽입 시 데이터베이스에서 null값을 가져오면 'None'으로 초기화
 
4. merge_table 생성
 > 기존 컬럼에서 dayoff(4), overtime(2-4), busi_trip(4) 삭제
 > connect.hr_info에서 사번, 이름, 조직 가져와서 merge_table에 target_day 날짜 정보와 같이 데이터 추가
 > 3번과 동일하게 dataframe에 삽입 시 데이터베이스에서 null값을 가져오면 'None'으로 초기화
 > origin_table의 정보를 각 사원별로 merge table에 축적시키기
 > > [shift_cd], [work_type]은 따로 가져오기
 > 초과근무 내역과 [shift_cd], [work_type] 반영해서 계획시간 만들기
 > connect.at_att_inout에서 출퇴근정보 조회 후 inout에 반영 
 > > 대휴, 재택을 제외한 09-18 근무자들의 inout은 무조건 default로 0900-1800으로 설정
 > 계획시간과 출퇴근시간 반영해서 확정시간 만들기
 > 확정시간과 [shift_cd], [work_type]을 기준으로 초과근무, 급량비 계산
 > > 3급 이상 또는 4급 팀장인 경우 (또는 일반직이면서 연봉제인 사원) 확정시간이 아니라 inout을 기준으로 급량비 계산
 
5. 중계 DB에 merge_table result push
 > Windows에서는 good.ehr_cal_test로, Linux(중계 DB server) 에서는 connect.ehr_cal로 insert
 
6. Additional Requirements & Feedback
 > 대휴와 주말근무를 정확히 구분하여 로직 반영
 > 육아기/임신기 근로단축 시 초과근무에 대한 기준 명확히 할 것
 
7. 기타 참고사항
 > 신청 TYPE
 > > 1008 : 초과근무 1004 : 기타 근무 1010 : 출장신청 1002 : 연차신청 1044 : 재택


</br></br>
두 가지 의존 파일 생성해야 함

1. login_info.py
```python
cx_Oracle_info={
    "id":"{ORACLE_USERNAME}",
    "pw":"{ORACLE_PASSWORD}",
    "host":"{ORACLE_HOST}"
}

pymysql_info={
    "host":"{MYSQL_HOST}"
    'user':"{MYSQL_USERNAME}"
    'password':"{MYSQL_PASSWORD}",
    'db':"{MYSQL_DATABASE}",
    'charset':'utf8'
}
```

2. integrated/db.py
``` python
import os
def check_location(os_name): # 운영체제에 맞는 client location 가져오기 (윈도우, 리눅스)
    if os_name=='Windows':
        return 'instantclient_21_3'
    elif os_name=='Linux':
        return 'instantclient_21_6'

# instantclient 환경변수 추가할 때 필요한 디렉토리명 리턴

# Windows에서는 v21_3, Linux(중계 DB server) 에서는 v21_4 사용 중

cx_Oracle_info={
    "id":"{ORACLE_USERNAME}",
    "pw":"{ORACLE_PASSWORD}",
    "host":"{ORACLE_HOST}"
}

pymysql_info={
    "host":"{MYSQL_HOST}"
    'user':"{MYSQL_USERNAME}"
    'password':"{MYSQL_PASSWORD}",
    'db':"{MYSQL_DATABASE}",
    'charset':'utf8'
}

col_inout_table=['EMP_CODE','WORK_DATE','WORK_CD','WORK_INFO','WORK_INFO_DAY','WORK_INFO_CLOCK','HOME_WORK_YN']

col_origin_table=['EMP_ID','APPR_YMD','YMD','STA_HM','END_HM','TYPE','APPL_ID','DEL_YN','BF_APPL_ID','APPL_TXT','REWARD_TYPE', 'RSN']


col_merge_table=['YMD','EMP_ID', 'NAME','ORG_NM','SHIFT_CD','WORK_TYPE','PLAN1','INOUT', 'FIX1','ERROR_INFO','DAYOFF1_TIME',
 
                 'DAYOFF1_ID','DAYOFF2_TIME','DAYOFF2_ID','OVER1_TIME','OVER1_ID','BUSI_TRIP1_TIME','BUSI_TRIP1_ID',

                 'BUSI_TRIP2_TIME','BUSI_TRIP2_ID','HOME_ID','ETC_INFO','ETC_ID','REWARD_TIME','REWARD_ID','CAL_OVERTIME',

                 'CAL_MEAL','RSN']

col_insert_table=['EMP_ID','SHIFT_CD','WORK_TYPE']

col_first_table=['EMP_ID','over_std_time','dayoff_std_time', 'dayoff_rest_time','p_edu_std_time','p_edu_admit_time',
                 'i_edu_std_time','i_edu_admit_time']

col_second_table=['EMP_ID','NAME','TOTAL_OVERTIME']

col_first_merged_table=['EMP_ID','NAME','TOTAL_OVERTIME','over_std_time','dayoff_std_time', 'dayoff_rest_time','p_edu_std_time','p_edu_admit_time',
                 'i_edu_std_time','i_edu_admit_time']
```