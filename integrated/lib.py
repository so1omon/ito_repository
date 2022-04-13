import datetime

def isDate(*args): # YYYY-MM-DD 유효성 판별
    print()
    flag=1
    for text in args:
        try: 
            datetime.datetime.strptime(text,"%Y%m%d") 
        except ValueError: 
            print("Incorrect data format({0}), should be YYYYMMDD".format(text)) 
            flag=0
    if args[0]>args[1]: # 날짜 순서가 뒤바뀐 경우
        print("Error : 시작 기간은 끝 기간보다 이전이어야 합니다.") 
        flag=0
    
    
    if flag!=1:
        return False
    else: 
        return True

def work_time(shift_cd, work_type): # shift_cd와 work_type을 넣으면 근무시간, 재택근무여부, 주말여부를 알려줌
    shift_cd_dict={'0010':'0700~1600', '0020':'0800:1700',}