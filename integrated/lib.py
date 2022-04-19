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
    if interval=='~':
        return ['','~','']
    elif len(interval)!=9:
        if interval[0]=='~':
            return ['None','~',interval[1:]]
        elif interval[4]=='~':
            return [interval[:4],'~','None']
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
    
def get_fixtime(inout, plan, work_type): # inout 한쪽이라도 유실된 데이터는 들어오지 않음
    temp_state=work_state(work_type)["work_time"]
    inout=sep_interval(inout)
    plan=sep_interval(plan)
    std_start,std_end=temp_state[0],temp_state[1] # 기준근로시간
    inout_start,inout_end=inout[0],inout[2] # 출퇴근기록
    plan_start,plan_end=plan[0],plan[2] # 계획시간
    
    # 계획시간에 맞춰 컷하기
    inout_start=max(inout_start, plan_start)
    inout_end=min(inout_end, plan_end)
    
    if work_type=='0060': # 주말근무
        return merge_interval([inout_start, inout_end])
    elif inout_start>std_start or inout_end<std_end: # 기준근로시간 충족하지 않을 때 (지각 또는 도망)
        return 'ERROR'
    else:
        if str_to_min(inout_start)>str_to_min(std_start)-30: # 출근시간이 기준 근로시작시간보다 30분 이상 선행되지 않을 때
            inout_start=std_start # 기준 근로시작시간으로 재설정
        if str_to_min(inout_end)<str_to_min(std_end)+30: # 출근시간이 기준 근로시작시간보다 30분 이상 선행되지 않을 때
            inout_start=std_start # 기준 근로시작시간으로 재설정
        return merge_interval([inout_start, inout_end])
    
    
    

    

if __name__=="__main__":
    inout='0900~1800'
    plan='0900~1800'
    work_type='0030'
    
    print(get_fixtime(inout, plan, work_type))
    


    