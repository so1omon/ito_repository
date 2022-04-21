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
        inout='~'
        emp_id=merge_table.loc[i,'EMP_ID']
        cur.execute(f"""SELECT WORK_INFO_CLOCK FROM connect.at_att_inout
                    WHERE EMP_CODE={emp_id} AND WORK_DATE ={today}
                    AND WORK_CD = 'IN' 
                    ORDER BY WORK_INFO_CLOCK LIMIT 1""") 
        for line in cur:   # 출근시간
            inout = line[0]+inout
        
        cur.execute(f"""SELECT WORK_INFO_CLOCK FROM connect.at_att_inout
                    WHERE EMP_CODE={emp_id} AND WORK_DATE ={today}
                    AND WORK_CD = 'OUT' 
                    ORDER BY WORK_INFO_CLOCK DESC LIMIT 1""") 
        for line in cur:   # 퇴근시간
            inout = inout + line[0]
        
        
        if merge_table.loc[i,'WORK_TYPE']=='0030': # 09~18 평일 근무자들은 기본값 09~18로 세팅
            inout_start=lib.sep_interval(inout)[0]
            inout_end=lib.sep_interval(inout)[2]
            if inout_start=='' or inout_start>'0900':
                inout_start='0900'
            if inout_end=='' or inout_end<'1800':
                inout_end='1800'
            inout=lib.merge_interval(inout_start, inout_end)
        
        merge_table.at[i,"INOUT"]=inout 
        # inout에 출근시간(xxxx)~퇴근시간(xxxx)형태로 전달
    return merge_table    
    

def make_fix(merge_table): # 확정시간 만들기
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
        
def get_fixtime(idx, merge_table): # 출장, 연차 처리 후 확정 시간 최종결정
    temp_state=lib.work_state(merge_table.loc[idx, "WORK_TYPE"])
    temp_fix=lib.sep_interval(merge_table.loc[idx, "FIX1"])
    plan=lib.sep_interval(merge_table.loc[idx, "PLAN1"])
    std_start,std_end=temp_state[0],temp_state[1] # 기준근로시간
    fix_start,fix_end=temp_fix[0],temp_fix[2] # 출퇴근기록
    plan_start,plan_end=plan[0],plan[2] # 계획시간
    
    error=merge_table.loc[idx,"ERROR_INFO"]
    
    # 계획시간에 맞춰 컷하기
    fix_start=max(fix_start, plan_start)
    fix_end=min(fix_end, plan_end)
    
    if temp_state["work_weekend"]: # 주말근무
        merge_table.at[idx,"FIX1"]=lib.merge_interval([fix_start, fix_end]) # 앞단에서 잘 처리했기 때문에 그대로 리턴
    
    else:
        if error!='': # 에러정보가 포함되어 있으면 그대로 넘기기
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
                if lib.str_to_min(fix_end)<lib.str_to_min(std_end)-30: # 출근시간이 기준 근로시작시간보다 30분 이상 선행되지 않을 때   
                    fix_end=std_end
                else:
                    fix_end=min(fix_end, plan_end)
            else:
                if error!='':
                    merge_table.at[idx,"ERROR_INFO"]=error+', 무단퇴근'
                else:
                    merge_table.at[idx,"ERROR_INFO"]='무단퇴근'
                    
        if fix_start=='' or fix_end =='':
            merge_table.at[idx, "ERROR"]='출근 또는 퇴근 유실' 
            
        merge_table.at[idx,"FIX1"]=lib.merge_interval([fix_start, fix_end])