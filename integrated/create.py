from numpy import size
import pandas as pd
import lib


# 계획시간(plan1,plan2) 설정하는 함수

# 초과근무 있는 경우 계획시간 만들기
def overToPlan(overtime,data):
    over_start, over_end = overtime.split('~')
    over_start = min(data["work_time"][0],over_start)     #더 작은 시간이 초과근무 시작시간
    over_end = max(data["work_time"][1],over_end)         #더 많은 시간이 초과근무 끝시간
    plantime = over_start+'~'+over_end
    return plantime                                     #'0000~0000'

def make_plan(merge_table):
    for i in range(len(merge_table)):
        # shift_cd = merge_table.loc[i]['SHIFT_CD']
        work_type = merge_table.loc[i]['WORK_TYPE']
        overtime = merge_table.loc[i]['OVER1_TIME']
        reward_time = merge_table.loc[i]['REWARD_TIME']
        
        data = lib.work_state(work_type)        # {work_time,work_home,work_weekend}
        if overtime != 'None':
            # 초과근무시간이 있는 경우 OverToPlan 함수 적용
            planTime = overToPlan(overtime,data)
            
        elif merge_table.loc[i]['REWARD_TIME']!='None':
            #휴일근무자인 경우, overtime은 없고 Reward time을 planTime으로 가짐
            planTime =  reward_time
                
        else:
            #초과근무시간도 없고 reward time 도 없는 경우, work type에 따라서 결정
            planTime = data["work_time"][0]+'~'+data["work_time"][1]
        
        merge_table.at[i,'PLAN1'] = planTime      #plan1 설정\
    return merge_table
    
    
def insert_inout(today,merge_table, cur): #  기록기 시간 생성

    for i in range(len(merge_table)):
        inout=''
        emp_id=merge_table.loc[i,'EMP_ID']
        cur.execute(f"""SELECT WORK_INFO_CLOCK FROM connect.at_att_inout
                    WHERE EMP_CODE={emp_id} AND WORK_DATE ={today}
                    AND WORK_CD = 'IN' 
                    ORDER BY WORK_INFO_CLOCK LIMIT 1""") 
        for line in cur:   # 출근시간
            inout = line[0]    
        
        inout=inout+'~'
        
        cur.execute(f"""SELECT WORK_INFO_CLOCK FROM connect.at_att_inout
                    WHERE EMP_CODE={emp_id} AND WORK_DATE ={today}
                    AND WORK_CD = 'OUT' 
                    ORDER BY WORK_INFO_CLOCK DESC LIMIT 1""") 
        for line in cur:   # 퇴근시간
            inout = inout + line[0]
        
        if inout !='~':
            merge_table.at[i,"INOUT"]=inout 
        
        # inout에 출근시간(xxxx)~퇴근시간(xxxx)형태로 전달, 출퇴근시간 모두 존재하지 않으면 아무 값도 넣지 않음
    return merge_table    
    
    

def make_inout(merge_table):
    for mem in range(len(merge_table)):
        time_list=[]
        time_list = findFreeTime(mem,merge_table)   # 각 사원의 연차 출장 정보 list
        new_list = lib.get_freetime(time_list)      #work state
        in_out = setInOut(mem,merge_table,new_list)             # inout 시간 확정
        merge_table.at[mem,"FIX1"] =in_out             
    
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
    return time_list

def setInOut(mem,merge_table,new_list):
    in_out = merge_table.at[mem,'INOUT']
    # 연차, 출장 정보를 보고 inout 확정 짓는 함수
    # list 길이 : 0,1,2 중 하나
    if len(new_list)==0:
        # 연차,출장 정보 없으면 inout 그대로 
        return in_out
  
    else:
        #있으면 덮어씌우기
        
        if(in_out!='None'):
            in_time,out_time = merge_table.at[mem,'INOUT'].split('~')
            print(in_time + '~'+out_time)
            first_start_time,first_end_time= new_list[-1].split('~')   #end_time>= in_time then  in_time = min( in_time,start_time)
            sec_start_time,sec_end_time = new_list[0].split('~')
            if first_end_time>=in_time and sec_start_time<=out_time: 
                in_time = min(in_time,first_start_time)
                out_time = max(out_time,sec_end_time)
                in_out = in_time+'~'+out_time 
                return in_out
            else:
                # 아닌 경우 error 처리
                in_out = merge_table.at[mem,'INOUT']
                merge_table.at[mem,'FIX1']="ERR"

        else:
            return in_out
        
        
def get_fixtime(idx, merge_table): # inout 한쪽이라도 유실된 데이터는 들어오지 않음 inout, plan, work_type
    temp_state=lib.work_state(merge_table.loc[idx, "WORK_TYPE"])
    inout=lib.sep_interval(merge_table.loc[idx, "INOUT"])
    plan=lib.sep_interval(merge_table.loc[idx, "PLAN1"])
    std_start,std_end=temp_state[0],temp_state[1] # 기준근로시간
    inout_start,inout_end=inout[0],inout[2] # 출퇴근기록
    plan_start,plan_end=plan[0],plan[2] # 계획시간
    
    # 계획시간에 맞춰 컷하기
    inout_start=max(inout_start, plan_start)
    inout_end=min(inout_end, plan_end)
    
    if temp_state["work_weekend"]: # 주말근무
        return lib.merge_interval([inout_start, inout_end])
    elif inout_start>std_start or inout_end<std_end: # 기준근로시간 충족하지 않을 때 (지각 또는 도망)
        return 'ERROR'
    else:
        if lib.str_to_min(inout_start)>lib.str_to_min(std_start)-30: # 출근시간이 기준 근로시작시간보다 30분 이상 선행되지 않을 때
            inout_start=std_start # 기준 근로시작시간으로 재설정
        if lib.str_to_min(inout_end)<lib.str_to_min(std_end)+30: # 출근시간이 기준 근로시작시간보다 30분 이상 선행되지 않을 때
            inout_start=std_start # 기준 근로시작시간으로 재설정
        return lib.merge_interval([inout_start, inout_end])