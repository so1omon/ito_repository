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
    '0420':['1000','1600'],'0430':['0900','1600'],'0440':['0800','1500'],'0450':['1000','1700'],'0021':['0830','1730'],
    '0031':['0930','1830'],'0460':['0900','1600'],'0470':['0800','1500'],'0480':['1000','1700'],'0050':['0900','1800'],
    '0011':['0730','1630'],'0271':['0730','1630'],'0281':['0830','1730'],'0291':['0930','1830']
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
    elif(work_type>='0270' and work_type<='0420')or(work_type>='0460' and work_type<='0480') :
        result["work_home"]=True
    
    return result

def sep_interval(interval): # xxxx~xxxx 포맷의 시간 간격을 분리해주는 함수
    if (interval=='~') or (interval=='None') or (interval=='ERROR'):
        return ['','~','']
    elif len(interval)!=9:
        if interval[0]=='~':
            return ['','~',interval[1:]]
        elif interval[4]=='~':
            return [interval[:4],'~','']
    return [interval[:4],'~',interval[5:]]

def merge_interval(args):
    if len(args)==2: # ['xxxx','xxxx'] format
        return args[0]+'~'+args[1]
    elif len(args)==3 and args[1]=='~': # ['xxxx','~','xxxx'] format
        return args[0]+'~'+args[2]

def get_freetime(time_list): # 한 직원의 특정 날짜에 해당하는 모든 출장 및 연차 정보를 가져와서 가공 후 리턴
    results=list(map(sep_interval, time_list))
    results=sorted(results, key=lambda result:result[0]) # 시작 시간에 대해서 정렬

    i=0 # iterator
    while(i<len(results)-1): # 연결되는 interval 합치기
        if results[i][2]==results[i+1][0]: # 끝시간과 시작시간이 이어질 때 합치기
            results[i]=[results[i][0],'~',results[i+1][2]]
            del results[i+1]
            i=i-1
        i=i+1
    if len(results)>=2: # 병합 후의 출장 연차 정보가 2개 이상일 때 처리
        results=[results[0],results[len(results)-1]]
    results=list(map(merge_interval, results))
    return results

def str_to_min(time): # 'xxxx' 4자리 시간 string을 분 단위로 교체
    return int(time[:2])*60+int(time[2:])

def min_to_str(time): # 분 단위 정수값을 'xxxx' 4자리 시간 string으로 교체
    return str(int(int(time)/60)).zfill(2)+str(int(int(time)%60)).zfill(2)

def sub_time(str1, str2): # 'xxxx' 4자리 시간 string 2개를 받아서 그 차이를 리턴, 최솟값은 '0000'
    if (str1=='') or (str2=='') or (str1<str2):
        return '0000'
    start, end=str_to_min(str1), str_to_min(str2)
    result=max(0,abs(start-end))
    
    return min_to_str(result)

def add_time(str1, str2): # 'xxxx' 4자리 시간 string 2개를 받아서 그 합을 리턴, 최솟값은 '0000'
    start, end=str_to_min(str1), str_to_min(str2)
    
    result=max(0,start+end)
    
    return min_to_str(result)
    
# 초과근무 있는 경우 계획시간 만들기
def overToPlan(overtime,data):
    over_start, over_end = overtime.split('~')
    # 초과근무 시간이 기존근로시간 벗어난 경우 plantime = 'None'
    if over_start>data['work_time'][1]:
        plantime='None'
        
    else:
        over_start = min(data["work_time"][0],over_start)     #더 작은 시간이 초과근무 시작시간
        over_end = max(data["work_time"][1],over_end)         #더 많은 시간이 초과근무 끝시간
        plantime = over_start+'~'+over_end
    return plantime                                     #'0000~0000' or None

def work_state_dic(row): # 하나의 row를 넘겨주면 fix, plan, std 등등의 정보를 6개의 변수로 나눠서 리턴해줌
    temp_state=work_state(row['WORK_TYPE'])
    temp_fix=sep_interval(row['FIX1'])
    plan=sep_interval(row['PLAN1'])
    std_start,std_end=temp_state['work_time'][0],temp_state['work_time'][1] # 기준근로시간 
    fix_start,fix_end=temp_fix[0],temp_fix[2] # 출퇴근기록
    plan_start,plan_end=plan[0],plan[2] # 계획시간
    if std_start=='None' and std_end=='None': # 주말근무일 경우 '~'로 리턴
        std_start,std_end='',''
    return temp_state,std_start,std_end,fix_start,fix_end,plan_start,plan_end

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return (next_month - datetime.timedelta(days=next_month.day)).strftime('%Y%m%d')

def first_day_of_month(any_day):
    return any_day.replace(day=1).strftime('%Y%m%d')

def month_interval(any_day):
    return [ first_day_of_month(any_day),last_day_of_month(any_day)]

if __name__=="__main__":
    print(sub_time('0000','0100'))


    