import os
import platform

def check_os(): # 운영체제명 가져오기 (윈도우, 리눅스)
    os_name=platform.system() 
    if os_name=='Windows':
        return 'instantclient_21_3'
    elif os_name=='Linux':
        return 'instantclient_21_4'

cx_Oracle_info={ # eHR Oracle DB 접속 정보
    "id":"silver",
    "pw":"silver",
    "host":"192.168.20.13:1521/IDTCORA",
    "LOCATION": check_os() 
    # instantclient 환경변수 추가할 때 필요한 디렉토리명 리턴
    # Windows에서는 v21_3, Linux(중계 DB server) 에서는 v21_4 사용 중
}

pymysql_info={ # 중계 DB (Maria) 접속 정보
    'host':'192.168.20.19',
    'user':'root',
    'password':'Azsxdc123$',
    'db':'connect',
    'charset':'utf8'
}

if __name__=='__main__': # login_info.py 실행 시
    print('Login_info offers the access information of eHR & relay DB.')
    