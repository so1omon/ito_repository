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
        
        data = lib.work_state(work_type)        # {work_time,work_home,work_weekend}
        if overtime != 'None':
                # 초과근무시간이 있는 경우 OverToPlan 함수 적용
            planTime = overToPlan(overtime,data)
            
        else:
                #초과근무시간이 없는 경우
            planTime = data["work_time"][0]+'~'+data["work_time"][1]
        
        print('변경 전 : '+merge_table.loc[i]['PLAN1'])
        print('plantime : '+planTime)
        merge_table.at[i,'PLAN1'] = planTime      #plan1 설정
        print('변경 후 : '+merge_table.loc[i]['PLAN1'])
        print()
        merge_table.at[i,'PLAN2'] = planTime      #plan2 설정
    print(merge_table['PLAN1'])
    
    
def insert_inout(today,merge_table, cur): #  기록기 시간 생성
    
    for i in range(len(merge_table)):
        emp_id='20170004'
        #merge_table.loc[i,'EMP_ID']
        inout=''
        print(emp_id)
        cur.execute(f"""SELECT WORK_INFO_CLOCK FROM connect.at_att_inout
                    WHERE EMP_CODE={emp_id} AND WORK_DATE ={today}
                    AND WORK_CD = 'IN' 
                    ORDER BY WORK_INFO_CLOCK LIMIT 1""") 
        
        cur.execute(f"""SELECT WORK_INFO_CLOCK FROM connect.at_att_inout
                    WHERE EMP_CODE={emp_id} AND WORK_DATE ={today}
                    AND WORK_CD = 'IN' 
                    ORDER BY WORK_INFO_CLOCK LIMIT 1""") 

        # print(cur.fetchall())
    