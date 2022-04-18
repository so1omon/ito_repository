import pandas as pd
import lib, test
pd.set_option
def make_plan(merge_table):
    for i in range(len(merge_table)):
        # shift_cd = merge_table.loc[i]['SHIFT_CD']
        work_type = merge_table.loc[i]['WORK_TYPE']
        overtime = merge_table.loc[i]['OVER1_TIME']
        
        data = lib.work_state(work_type)        # {work_time,work_home,work_weekend}
        if overtime != 'None':
                # 초과근무시간이 있는 경우 OverToPlan 함수 적용
            planTime = test.overToPlan(overtime,data)
            
        else:
                #초과근무시간이 없는 경우
            planTime = data["work_time"][0]+'~'+data["work_time"][1]
        
        merge_table.loc[i]['PLAN1'] = planTime      #plan1 설정
        merge_table.loc[i]['PLAN2'] = planTime      #plan2 설정
    
    
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
    
    