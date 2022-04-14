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
        
        print('변경 전 : '+merge_table.loc[i]['PLAN1'])
        print('plantime : '+planTime)
        merge_table.loc[i]['PLAN1'] = planTime      #plan1 설정
        print('변경 후 : '+merge_table.loc[i]['PLAN1'])
        print()
        merge_table.loc[i]['PLAN2'] = planTime      #plan2 설정
    print(merge_table['PLAN1'])