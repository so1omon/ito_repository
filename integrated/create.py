from numpy import size
import pandas as pd
import lib
import db


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
        merge_table.at[i,'PLAN1'] = planTime      #plan1 설정\
    return merge_table
    
    
def insert_inout(today,merge_table, cur): #  기록기 시간 생성
    cur.execute(f"""SELECT * FROM connect.at_att_inout
                    WHERE WORK_DATE ={today}""")
    x=cur.fetchall()
    inout_table=pd.DataFrame(x)
    inout_table.columns=db.col_inout_table
    
    for i in range(len(merge_table)):
        inout='~'
        emp_id=merge_table.loc[i,'EMP_ID']
        print(emp_id)
        target_in_table=inout_table[(inout_table["EMP_CODE"]==emp_id) & (inout_table["WORK_CD"]=='IN')].sort_values(by='WORK_INFO_CLOCK', ascending=True).reset_index(drop=True)
        target_out_table=inout_table[(inout_table["EMP_CODE"]==emp_id) & (inout_table["WORK_CD"]=='out')].sort_values(by='WORK_INFO_CLOCK', ascending=False).reset_index(drop=True)
        if len(target_in_table)!=0:   # 출근시간
            inout = target_in_table.loc[0, "WORK_INFO_CLOCK"]+inout
        
        if len(target_out_table)!=0:   # 출근시간
            inout = inout+target_out_table.loc[0, "WORK_INFO_CLOCK"]
        if merge_table.loc[i,'WORK_TYPE']=='0030': # 09~18 평일 근무자들은 기본값 09~18로 세팅
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
    

def make_fix(merge_table): # 확정시간 만들기
    for mem in range(len(merge_table)):
        time_list=[]
        time_list = findFreeTime(mem,merge_table)   # 각 사원의 연차 출장 정보 list
        new_list = lib.get_freetime(time_list)      #work state
        setInOut(mem,merge_table,new_list)             # inout 시간 확정        
        get_fixtime(mem, merge_table)
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
        time_list.append(work_time)
        
    return time_list

def setInOut(mem,merge_table,new_list):
    in_out = merge_table.at[mem,'INOUT']        #in_out : 
    work_type = merge_table.loc[mem,'WORK_TYPE']
    work_state = lib.work_state(work_type)
    
    # new_list(출장,연차 리스트)의 유효성 판별
    for i in range(len(new_list)):
        if new_list[i][0]>work_state["work_time"][0] and new_list[i][1]<work_state["work_time"][1]:
            # 유효하지 않은 출장,연차 정보인 경우 list에서 삭제
            del new_list[i]
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
            
            #  1개인 경우
            if len(list)==1:
                start_time = lib.sep_interval(list[0])[0]       #XXXX
                end_time = lib.sep_interval(list[0])[2]         #XXXX
                if start_time == work_state["work_time"][0] and end_time == work_state["work_time"][1]:
                    # 전일 연차인 경우, fix1에 그대로 넣음
                    in_out = lib.merge_interval([start_time,end_time])          
                else:
                    if in_time<=end_time:
                        in_time = min(in_time,start_time)
                        if out_time =='':
                            out_time = min(out_time,end_time)
                        else:
                            out_time = max(out_time,end_time)
                        in_out = lib.merge_interval([in_time,out_time])     
                merge_table.at[mem,'FIX1']= in_out
                    
            #  2개인 경우
            elif len(list)==2:
                start_time = [list[i][0] for i in range(len(list))]         # start_time[0]='XXXX' start_time[1]='XXXX'
                end_time = [list[i][2]for i in range(len(list))]     
                if end_time[0]>=in_time : 
                    in_time = min(in_time,start_time[0])
                    if out_time =='':
                        # 공백인 경우 그대로 공백 
                        out_time = min(out_time,end_time[1])
                    else:
                        # 공백이 아닌 경우, out_time이 start_time[1] 보다 늦어야 함.
                        if out_time>=start_time[1]:
                            out_time = max(out_time,end_time[1])
                        else:
                            #  공백이 아닌 경우, out time이 start_time[1] 보다 빠르면  out_time 그대로 
                            out_time = out_time
                    in_out = lib.merge_interval([in_time,out_time])
                    merge_table.at[mem,'FIX1']= in_out
                else:
                    merge_table.at[mem,'FIX1']= in_out
                # 아닌 경우 error 처리
                    
        
def get_fixtime(idx, merge_table): # 출장, 연차 처리 후 확정 시간 최종결정
    temp_state=lib.work_state(merge_table.loc[idx, "WORK_TYPE"])
    temp_fix=lib.sep_interval(merge_table.loc[idx, "FIX1"])
    plan=lib.sep_interval(merge_table.loc[idx, "PLAN1"])
    std_start,std_end=temp_state['work_time'][0],temp_state['work_time'][1] # 기준근로시간 
    fix_start,fix_end=temp_fix[0],temp_fix[2] # 출퇴근기록
    plan_start,plan_end=plan[0],plan[2] # 계획시간
    
    error=merge_table.loc[idx,"ERROR_INFO"]
    
    # 계획시간에 맞춰 컷하기
    fix_start=max(fix_start, plan_start)
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

def get_overtime(idx, merge_table):
    cal_overtime_start='' # 앞단의 초과근무시간
    cal_overtime_end=''
    temp_fix=lib.sep_interval(merge_table.loc[idx, "FIX1"])
    fix_start,fix_end=temp_fix[0],temp_fix[2] # 출퇴근기록
    
    
    
    #1. 양쪽 유실되었을 때
    #2. 한쪽 유실되었을 때
    #3. 정상적일 때 (ERROR_INFO 있을 때와 없을 때 나눠서)
    
    

    