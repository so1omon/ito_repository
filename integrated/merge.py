import lib, pandas as pd

dayoff_string_list=['DAYOFF1_TIME','DAYOFF2_TIME']
dayoff_string_id_list=['DAYOFF1_ID','DAYOFF2_ID']
overtime_string_list=['OVER1_TIME']
overtime_string_id_list=['OVER1_ID']
busitrip_string_list=['BUSI_TRIP1_TIME','BUSI_TRIP2_TIME']
busitrip_string_id_list=['BUSI_TRIP1_ID','BUSI_TRIP2_ID']


def origin_to_merge(origin_table, merge_table):
    for idx in range(len(origin_table)):
        rows_origin=origin_table.loc[idx] # origin table 1행
        
        cond_emp_id=merge_table['EMP_ID']==rows_origin['EMP_ID'] # 같은 사번을 가진 행 찾기
        merge_index=merge_table.loc[cond_emp_id,'EMP_ID'].keys()[0] #merge table과 사번 일치하는 행 인덱스넘버
        insert_flag=0 # 값이 삽입되었다는 것을 알리는 플래그. 1이 되면 string list 탐색을 중지하고 다음 origin table튜플을 탐색
        
        if rows_origin['TYPE']=='1008': #초과근무
            if rows_origin['STA_HM']=='None': # 널값 들어 있을 때
                continue
            temp_time=lib.merge_interval([rows_origin['STA_HM'],rows_origin['END_HM']]) # xxxx~xxxx
            
            if rows_origin['REWARD_TYPE']!='None': # reward type이 정의되어 있을 때(주말)
                merge_table.at[merge_index,'REWARD_TIME']=temp_time
                merge_table.at[merge_index,'REWARD_ID']=rows_origin['REWARD_TYPE']
                merge_table.at[merge_index,'RSN']=rows_origin['RSN']
                continue
            
            for isvalue in overtime_string_list: #merge table 슬롯 하나씩 채우기
                if insert_flag==1:
                    break
                
                if merge_table.loc[merge_index,isvalue]=='None':
                    insert_flag=1
                    merge_table.at[merge_index,isvalue]=temp_time
                    merge_table.at[merge_index,overtime_string_id_list[overtime_string_list.index(isvalue)]]=rows_origin['APPL_ID']
                    merge_table.at[merge_index,'RSN']=rows_origin['RSN']

        elif rows_origin['TYPE']=='1002': #연차
            if rows_origin['STA_HM']=='None':
                continue
            temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
            for isvalue in dayoff_string_list:
                if insert_flag==1:
                    break
                if merge_table.loc[merge_index,isvalue]=='None':
                    insert_flag=1
                    merge_table.at[merge_index,isvalue]=temp_time
                    merge_table.at[merge_index,dayoff_string_id_list[dayoff_string_list.index(isvalue)]]=rows_origin['APPL_ID']
                    continue
                
                if isvalue==dayoff_string_list[-1]:
                    raise Exception('연차 신청 내역이 3개 이상입니다.')#예외 발생 
                
        elif rows_origin['TYPE']=='1010': #출장
            temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
            for isvalue in busitrip_string_list:
                if insert_flag==1:
                    break
                
                if merge_table.loc[merge_index,isvalue]=='None':
                    insert_flag=1
                    merge_table.at[merge_index,isvalue]=temp_time
                    merge_table.at[merge_index,busitrip_string_id_list[busitrip_string_list.index(isvalue)]]=rows_origin['APPL_ID']                
                    
                if isvalue==busitrip_string_list[-1]:
                    raise Exception('연차 신청 내역이 3개 이상입니다.')#예외 발생 

        elif rows_origin['TYPE']=='1044': #재택
            merge_table.at[merge_index, 'HOME_ID']=rows_origin['APPL_ID']
            
        elif rows_origin['TYPE']=='1004': #기타휴가 
            merge_table.at[merge_index, 'ETC_INFO']=rows_origin['APPL_TXT']
            merge_table.at[merge_index, 'ETC_ID']=rows_origin['APPL_ID']
    return merge_table


# merge_table.to_csv('integrated/data/scheduled_query_merge_table.csv',
#                            sep=',',na_rep='NaN', float_format = '%.2f', # 2 decimal places
#                            index=False,encoding='utf-8-sig')

