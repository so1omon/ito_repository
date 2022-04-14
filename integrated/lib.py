import datetime, pandas
from functools import partial

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


work_type_dict={
    '0010':['0700','1600'],'0020':['0800','1700'],'0030':['0900','1800'],'0040':['1000','1900'],'0060':['None','None'],
    '0070':['0700','1500'],'0080':['0700','1400'],'0090':['0700','1300'],'0100':['0800','1600'],'0110':['0800','1500'],
    '0120':['0800','1400'],'0130':['0900','1700'],'0140':['0900','1600'],'0150':['0900','1500'],'0160':['1000','1800'],
    '0170':['1000','1700'],'0180':['1000','1600'],'0190':['0700','1400'],'0200':['0700','1300'],'0210':['0800','1500'],
    '0220':['0800','1400'],'0230':['0900','1600'],'0240':['0900','1500'],'0250':['1000','1700'],'0260':['1000','1600'],
    '0270':['0700','1600'],'0280':['0800','1700'],'0290':['0900','1800'],'0300':['1000','1900'],'0310':['0700','1500'],
    '0320':['0700','1400'],'0330':['0700','1300'],'0340':['0800','1600'],'0350':['0800','1500'],'0360':['0800','1400'],
    '0370':['0900','1700'],'0380':['0900','1600'],'0390':['0900','1500'],'0400':['1000','1800'],'0410':['1000','1700'],
    '0420':['1000','1600'],'0430':['0900','1600'],'0440':['0800','1500'],'0450':['1000','1700']
}

def work_state(work_type): # shift_cd와 work_type을 넣으면 근무시간, 재택근무여부, 주말여부를 알려줌
    result={
        "work_time":[],
        "work_home":False,
        "work_weekend":False
    }
    
    result["work_time"]=work_type_dict[work_type]
    if(work_type=='0060'):
        result["work_weekend"]=True
    elif(work_type>='0270' and work_type<='0420'):
        result["work_home"]=True
    
    return result

def sep_interval(interval): # xxxx~xxxx 포맷의 시간 간격을 분리해주는 함수
    if len(interval)!=9:
        if interval[0]=='~':
            return ['None','~',interval[1:]]
        elif interval[4]=='~':
            return [interval[:4],'~','None']
    return [interval[:4],'~',interval[5:]]

def merge_interval(*args):
    return args[0]+'~'+args[1]

make_csv=partial(pandas.DataFrame.to_csv, sep=',',na_rep='NaN', float_format = '%.2f', # 2 decimal places
                           index=False, encoding='utf-8-sig')


# if __name__=="__main__":
#     print(work_state(input()))


    