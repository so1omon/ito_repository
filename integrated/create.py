from logging import exception
from numpy import size
import pandas as pd
import lib
import db
import query
import pymysql




# 계획시간(plan1,plan2) 설정하는 함수

def make_plan(merge_table):
    for i in range(len(merge_table)):
        # shift_cd = merge_table.loc[i]['SHIFT_CD']
        work_type = merge_table.loc[i]['WORK_TYPE']
        overtime = merge_table.loc[i]['OVER1_TIME']
        reward_time = merge_table.loc[i]['REWARD_TIME']
        
        data = lib.work_state(work_type)        # {work_time,work_home,work_weekend}
        if overtime != 'None':
            # 초과근무시간이 있는 경우 OverToPlan 함수 적용
            planTime = lib.overToPlan(overtime,data)
            if planTime == 'None':
                # error처리, fix1='None'처리
                merge_table.at[i,'ERROR_INFO']='초과근무 오류'
                merge_table.at[i,'FIX1']='ERROR'
            
        elif merge_table.loc[i]['REWARD_TIME']!='None':
            #휴일근무자인 경우, overtime은 없고 Reward time을 planTime으로 가짐
            planTime =  reward_time
                
        else :
            #초과근무시간도 없고 reward time 도 없는 경우, work type에 따라서 결정
            planTime = data["work_time"][0]+'~'+data["work_time"][1]
        
        if planTime == "None~None":
            planTime = 'None'
        merge_table.at[i,'PLAN1'] = planTime      #plan1 설정
    return merge_table
    
    
def insert_inout(today,merge_table, cur): #  기록기 시간 생성
    cur.execute(f"""SELECT * FROM connect.at_att_inout
                    WHERE WORK_DATE ={today}""")
    x=cur.fetchall()
    inout_table=pd.DataFrame(x)
    if len(x)==0:
        merge_table['INOUT']='~'
        return merge_table
    inout_table.columns=db.col_inout_table
    
    for i in range(len(merge_table)):
        inout='~'
        emp_id=merge_table.loc[i,'EMP_ID']
        target_in_table=inout_table[(inout_table["EMP_CODE"]==emp_id) & (inout_table["WORK_CD"]=='IN')].sort_values(by='WORK_INFO_CLOCK', ascending=True).reset_index(drop=True)
        target_out_table=inout_table[(inout_table["EMP_CODE"]==emp_id) & (inout_table["WORK_CD"]=='OUT')].sort_values(by='WORK_INFO_CLOCK', ascending=False).reset_index(drop=True)
        if len(target_in_table)!=0:   # 출근시간
            inout = target_in_table.loc[0, "WORK_INFO_CLOCK"]+inout
        
        if len(target_out_table)!=0:   # 출근시간
            inout = inout+target_out_table.loc[0, "WORK_INFO_CLOCK"]
        if (merge_table.loc[i,'WORK_TYPE']=='0030') or (merge_table.loc[i,'WORK_TYPE']=='0050'): # 09~18 평일 근무자들은 기본값 09~18로 세팅
            inout_start=lib.sep_interval(inout)[0]
            inout_end=lib.sep_interval(inout)[2]
            if inout_start=='' or inout_start>'0900':
                inout_start='0900'
            if inout_end=='' or inout_end<'1800':
                inout_end='1800'
            inout=lib.merge_interval([inout_start, inout_end])    
            
        merge_table.at[i,"INOUT"]=inout 
        # inout에 출근시간(xxxx)~퇴근시간(xxxx)형태로 전달
    return merge_table
    

def make_fix(merge_table, exception_list): # 확정시간 만들기
    for mem in range(len(merge_table)):
        time_list=[]
        time_list = findFreeTime(mem,merge_table)   # 각 사원의 연차 출장 정보 list
        new_list = lib.get_freetime(time_list)      #work state
        setInOut(mem,merge_table,new_list)             # inout 시간 확정        
        get_fixtime(mem, merge_table)
        get_overtime(mem, merge_table)
        get_meal(mem, merge_table, exception_list)
    return merge_table
        
# findFreeTime(merge_table) : 해당 사원의 연차 출장 정보 list 리턴 함수

def findFreeTime(mem,merge_table):
    time_list = []
    if(merge_table.at[mem,'DAYOFF1_TIME']!='None'):
        # None이 아니면 time_list에 append
        time_list.append(merge_table.loc[mem,'DAYOFF1_TIME'])
    if(merge_table.at[mem,'DAYOFF2_TIME']!='None'):
        time_list.append(merge_table.loc[mem,'DAYOFF2_TIME'])
    if(merge_table.at[mem,'BUSI_TRIP1_TIME']!='None'):
        time_list.append(merge_table.loc[mem,'BUSI_TRIP1_TIME'])
    if(merge_table.at[mem,'BUSI_TRIP2_TIME']!='None'):
        time_list.append(merge_table.loc[mem,'BUSI_TRIP2_TIME'])
    if(merge_table.at[mem,'ETC_INFO']!='None'):
        # 기타휴가가 있는 경우
        work_state = lib.work_state(merge_table.loc[mem,'WORK_TYPE'])
        work_time = lib.merge_interval(work_state["work_time"])
        if work_state['work_weekend']:
            return time_list
        
        merge_table.at[mem,'DAYOFF1_TIME'] = work_time
         
        time_list.append(work_time)
        
    return time_list

def setInOut(mem,merge_table,new_list):
    in_out = merge_table.at[mem,'INOUT']        #in_out : 
    work_type = merge_table.loc[mem,'WORK_TYPE']
    work_state = lib.work_state(work_type)
    plan_time = merge_table.at[mem,'PLAN1']
    
    # new_list(출장,연차 리스트)의 유효성 판별
    
    iter_new_list=0
    while(iter_new_list<len(new_list)):
        if lib.sep_interval(new_list[iter_new_list])[0]>work_state["work_time"][0] and lib.sep_interval(new_list[iter_new_list])[2]<work_state["work_time"][1]:
            # 유효하지 않은 출장,연차 정보인 경우 list에서 삭제
            del new_list[iter_new_list]
            iter_new_list=iter_new_list-1
        iter_new_list=iter_new_list+1
        
    list = new_list
    

    # 연차, 출장 정보를 보고 inout 확정 짓는 함수
    # list 길이 : 0,1,2 중 하나
    #  error 있는 경우, fix1 바꾸지 않음
    if merge_table.loc[mem,'ERROR_INFO']!='None':
        pass
    else:
        if len(list)==0:
            # 연차,출장 정보 없으면 inout 그대로 
            merge_table.at[mem,'FIX1']= in_out
        else:
            #연차,출장 정보 있으면 덮어씌우기
            in_time = lib.sep_interval(in_out)[0]
            out_time = lib.sep_interval(in_out)[2]
            plan_start = lib.sep_interval(plan_time)[0]
            plan_end = lib.sep_interval(plan_time)[2]
            
            #  1개인 경우
            if len(list)==1:
                start_time = lib.sep_interval(list[0])[0]       #XXXX
                end_time = lib.sep_interval(list[0])[2]         #XXXX
                
                #work type 60이고 plan이 None이 아니면 
                if work_state["work_weekend"] and plan_time!='None':
                    work_state["work_time"]=[plan_start, plan_end]

                # 전일 연차 or 출장인 경우, 
                if start_time == work_state["work_time"][0] and end_time == work_state["work_time"][1]:
                    if in_time=='':
                        in_time = start_time
                    else:
                        in_time = min(in_time,start_time)
                    out_time = max(out_time,end_time)          
                else:
                    # 전일 연차 , 출장 아닌 경우.
                    # 1. 연차, 출장이 앞단인지 뒷단인지 확인
                    if end_time<plan_end:
                        # 앞단인 경우
                        if in_time == '':
                            in_time = start_time
                        else:
                            in_time = min(in_time,start_time)
                        out_time = max(out_time,end_time)
                    else:
                        # 뒷단인 경우 , in_time이 공백이라면 그대로 놔둠
                        if in_time!='':
                            in_time = min(in_time,start_time)
                        out_time = max(out_time,end_time)
                    
                in_out = lib.merge_interval([in_time,out_time])     
                merge_table.at[mem,'FIX1']= in_out
                
                                     
                
                    
            #  2개인 경우
            elif len(list)==2:
                start_time = [i[0] for i in map(lib.sep_interval, list)]          # start_time[0]='XXXX' start_time[1]='XXXX'
                end_time = [i[2] for i in map(lib.sep_interval, list)] 
                
                # in or out에 공백있는 경우
                if in_time=='':
                    in_time = start_time[0]
                else:
                    in_time = min(in_time,start_time[0])
                out_time = max(out_time,end_time[1])
                       
                in_out = lib.merge_interval([in_time,out_time])     
                merge_table.at[mem,'FIX1']= in_out           
                    
                    
        
def get_fixtime(idx, merge_table): # 출장, 연차 처리 후 확정 시간 최종결정
    
    temp_state,std_start,std_end,fix_start,fix_end,plan_start,plan_end=lib.work_state_dic(merge_table.loc[idx])
    
    error=merge_table.loc[idx,"ERROR_INFO"]
    
    # 계획시간에 맞춰 컷하기
    if fix_start!='':
        if plan_start=='':
            fix_start=''
        else:
            if fix_end!='':
                if(plan_end<=fix_start) or (plan_start>=fix_end):
                    fix_start=''
                    fix_end=''
                else:
                    fix_start=max(fix_start, plan_start)
    if fix_end!='':      
        if plan_end=='':
            fix_end=''  
        else:
            if fix_start!='':
                if(plan_end<=fix_start) or (plan_start>=fix_end):
                    fix_start=''
                    fix_end=''
                else:
                    fix_end=min(fix_end, plan_end)
    
    if temp_state["work_weekend"]: # 주말근무
        merge_table.at[idx,"FIX1"]=lib.merge_interval([fix_start, fix_end]) # 앞단에서 잘 처리했기 때문에 그대로 리턴
    
    else:
        if error!='None': # 에러정보가 포함되어 있으면 그대로 넘기기
            merge_table.at[idx,"FIX1"]='ERROR'
            return
        if fix_start!='':
            if fix_start<=std_start:
                if lib.str_to_min(fix_start)>lib.str_to_min(std_start)-30: # 출근시간이 기준 근로시작시간보다 30분 이상 선행되지 않을 때   
                    fix_start=std_start
                else:
                    fix_start=max(fix_start, plan_start)
            else:
                merge_table.at[idx,"ERROR_INFO"]='지각'
        if fix_end!='':
            if fix_end>=std_end:
                if lib.str_to_min(fix_end)<lib.str_to_min(std_end)+30: # 출근시간이 기준 근로시작시간보다 30분 이상 선행되지 않을 때   
                    fix_end=std_end
                else:
                    fix_end=min(fix_end, plan_end)
            else:
                if error!='None':
                    merge_table.at[idx,"ERROR_INFO"]=error+', 무단퇴근'
                else:
                    merge_table.at[idx,"ERROR_INFO"]='무단퇴근'
                    
        if fix_start=='' or fix_end =='':
            merge_table.at[idx, "ERROR_INFO"]='출근 또는 퇴근 유실' 
            
        merge_table.at[idx,"FIX1"]=lib.merge_interval([fix_start, fix_end])

def get_overtime(idx, merge_table): # 초과근무시간 산정
    # 2022-05-10 추가사항 : 임신기 근로단축은 일반근로시간만큼의 급여를 지급받기 때문에 초과근무를 계산할 떄
    # {9시간(일반 근로시간) - 단축근무시간}만큼 초과근무시간에서 빼줘야 함. 이런 변경 사항에 따라
    # 반영, 이에 따라 임신기 근로단축 (0430,0440,0450)에 해당하는 근태내역의 초과근무시간을 변경
    
    cal_overtime_start=0 # 앞단의 초과근무시간
    cal_overtime_end=0 # 뒷단의 초과근무시간
    temp_state,std_start,std_end,fix_start,fix_end,plan_start,plan_end=lib.work_state_dic(merge_table.loc[idx])
    omit_flag=0 # 누락건 있으면 1로 변경
    #lib.min_to_str
    
    if temp_state['work_weekend']:# 주말이면 확정시간 그대로 차이값 계산
        cal_overtime_start=lib.sub_time(fix_end,fix_start)
    else:
        #1. 앞단처리
        if fix_start!='':
            cal_overtime_start=lib.sub_time(std_start,fix_start)
        else:
            omit_flag=1
        #2. 뒷단처리
        if fix_end!='':
            cal_overtime_end=lib.sub_time(fix_end,std_end)
        else:
            omit_flag=1
    if cal_overtime_start==0:
        cal_overtime_start=lib.min_to_str(cal_overtime_start)
    if cal_overtime_end==0:
        cal_overtime_end=lib.min_to_str(cal_overtime_end)
    
    result=min(lib.add_time(cal_overtime_start,cal_overtime_end),'0400')
    if (temp_state["work_home"]==True) or (merge_table.loc[idx, 'REWARD_ID']=='400'): # 재택근무/대체휴일 시에는 초과근무 시간 0으로 설정
        result='0000'
    if (omit_flag==1) and (result!='0000'):
        merge_table.at[idx, 'ERROR_INFO']=merge_table.loc[idx, 'ERROR_INFO']+', 누락 건에 대한 초과근무 시간 인정'
    
    if merge_table.loc[idx, 'WORK_TYPE'] in ['0430','0440','0450']: # 2022-05-10 변경 (임신기근로단축)
        work_interval_time=lib.sub_time(std_end,std_start) # 임신기단축근무 근로시간
        work_interval_time_gap=lib.sub_time('0900',work_interval_time)
        
        if work_interval_time_gap>=result:
            result='0000'
        else:
            result=lib.sub_time(result, work_interval_time_gap)
    
    merge_table.at[idx, 'CAL_OVERTIME']=result
    
def get_meal(idx, merge_table, exception_list):
    merge_table.at[idx, 'CAL_MEAL']='FALSE'
    temp_state,std_start,std_end,fix_start,fix_end,plan_start,plan_end=lib.work_state_dic(merge_table.loc[idx])
    
    cond_1=merge_table.loc[idx,'CAL_OVERTIME']<'0100' # 초과근무 시간이 한시간 미만이면 급량비 산정 FALSE
    cond_2=merge_table.loc[idx,'FIX1']=='ERROR' # 잘못된 정보 들어가있으면 급량비 산정 FALSE
    cond_3=(fix_start=='') and (fix_end=='') # 출퇴근 모두 비어있으면 급량비 산정 FALSE -> 출퇴근 한쪽만 충족할 경우 물어보기
    cond_4=temp_state["work_home"]==True # 재택근무이면 초과근무 0시간이므로 급량비 산정 FALSE     
    cond_5=merge_table.loc[idx,'INOUT']=='~'# exception_list용 조건, 출퇴근 모두 비어있으면 끝
    
    if merge_table.loc[idx, 'EMP_ID'] in exception_list: # 3급 이상, 4급 팀장에 대한 예외 처리 -> 출퇴근 기준으로 보기
        if cond_2 or cond_3 or cond_4 or cond_5: # 초과근무 시간이 산정되지 못하기 때문에 cond_1 제외
            pass
        else:
            inout=lib.sep_interval(merge_table.loc[idx,'INOUT'])
            inout_start=inout[0]
            inout_end=inout[2]
            
            if (temp_state["work_weekend"]):
                if (inout_start=='') or (inout_end==''):
                    pass
                else:
                    if lib.sub_time(inout_end, inout_start)>='0200':
                        merge_table.at[idx, 'CAL_MEAL']='TRUE'
            else:
                # 한쪽 비어 있을 때 처리 (주말근무와 평일근무에 대해서)
                # 1. 주말근무 - 한쪽이라도 비어 있으면 끝, 그게 아니면 출퇴근시간 차이가 2시간 이상일 때 급량비 TRUE
                # 2. 평일근무 - 한쪽이라도 값이 존재하면 그 방향으로 급량비 판별
                if inout_start!='':
                    if inout_start<=min(lib.sub_time(std_start,'0100'),'0800'):
                        merge_table.at[idx, 'CAL_MEAL']='TRUE'
                if inout_end!='':
                    if inout_end>=max(lib.add_time(std_end,'0100'),'1900'):
                        merge_table.at[idx, 'CAL_MEAL']='TRUE'
        return # exception_list 예외처리 끝
    
    if cond_1 or cond_2 or cond_3 or cond_4: # 일반 직원 중 4가지 조건 중 하나라도 만족하면 해당 행 작업 종료
        return
    
    if (temp_state["work_weekend"]):
        if (merge_table.loc[idx, 'CAL_OVERTIME'] >= '0200'): # 주말근무일 때 2시간 이상이어야 TRUE
            merge_table.at[idx, 'CAL_MEAL']='TRUE'
        return # 일반직원 주말 근무 끝
    
    # 급량비 조건 만족하는지 (기준근로시간 기준 출근이 한시간 빠르거나 퇴근이 한시간 느릴 때 적용)
    if fix_end!='':
        if fix_end>=max(lib.add_time(std_end,'0100'),'1900'): 
            merge_table.at[idx, 'CAL_MEAL']='TRUE'
    if fix_start!='':
        if fix_start<=min(lib.sub_time(std_start,'0100'),'0800'):
            merge_table.at[idx, 'CAL_MEAL']='TRUE'