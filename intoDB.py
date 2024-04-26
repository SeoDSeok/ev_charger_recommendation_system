# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import glob
from collections import defaultdict
import psycopg2
from tqdm import tqdm
import requests
import xmltodict
import pandas as pd
from pandas.io.json import json_normalize
import schedule
import time
import datetime
import pytz
import copy
from psycopg2 import IntegrityError

def xml_to_json(xml_string):
    # XML 파싱
    xml_dict = xmltodict.parse(xml_string, dict_constructor=dict)
    # JSON으로 변환
    json_data = xml_dict

    return json_data

# +
def distribute(df):
    df_up =  df[['statId', 'chgerId', 'stat', 'statUpdDt', 'lastTsdt', 'lastTedt', 'nowTsdt']]
    df_up2 = df[['statId', 'chgerId', 'useTime', 'limitYn', 'limitDetail', 'delYn', 'delDetail']]
    df_non_up = df.drop(columns=['stat', 'statUpdDt', 'lastTsdt', 'lastTedt', 'nowTsdt', 'useTime', 'limitYn', 'limitDetail', 'delYn', 'delDetail'])
    return df_up, df_up2, df_non_up 

# Status Table과 새로운 Status Table을 비교해서 갱신된 값만 출력
def makeup(df):
    index = []
    for idx, val in enumerate(zip(df['statUpdDt_x'].values, df['statUpdDt_y'].values)):
        if val[0] != val[1]:
            index.append(idx)
    df.drop(columns = ['statNm_x', 'chgerType_x', 'addr_x', 'location_x',
       'lat_x', 'lng_x', 'useTime_x', 'busiId_x', 'bnm_x', 'busiNm_x',
       'busiCall_x', 'stat_x', 'statUpdDt_x', 'lastTsdt_x', 'lastTedt_x',
       'nowTsdt_x', 'powerType_x', 'output_x', 'method_x', 'zcode_x',
       'zscode_x', 'kind_x', 'kindDetail_x', 'parkingFree_x', 'note_x',
       'limitYn_x', 'limitDetail_x', 'delYn_x', 'delDetail_x', 'trafficYn_x'], inplace=True)
    new_column_names = {}
    for k in df.columns[13:18]:
        new_column_names[k] = k[:-2]
    df.rename(columns = new_column_names, inplace=True)
    df.drop(columns = ['statNm_y', 'chgerType_y', 'addr_y', 'location_y', 'lat_y', 'lng_y',
       'useTime_y', 'busiId_y', 'bnm_y', 'busiNm_y', 'busiCall_y', 'powerType_y',
       'output_y', 'method_y', 'zcode_y', 'zscode_y', 'kind_y', 'kindDetail_y',
       'parkingFree_y', 'note_y', 'limitYn_y', 'limitDetail_y', 'delYn_y',
       'delDetail_y', 'trafficYn_y', '_merge'], inplace=True)
    return df.loc[index]

# Status Table과 새로운 Status Table을 비교해서 갱신된 값만 출력 ver2
def makeup_status(df):
    index = []
    for idx, val in enumerate(zip(df['statUpdDt_x'].values, df['statUpdDt_y'].values)):
        if val[0] != val[1]:
            index.append(idx)
    df.drop(columns = ['stat_x', 'statUpdDt_x', 'lastTsdt_x', 'lastTedt_x', 'nowTsdt_x'], inplace=True)
    new_column_names = {}
    for k in df.columns[2:7]:
        new_column_names[k] = k[:-2]
    df.rename(columns = new_column_names, inplace=True)
    df.drop(columns = ['_merge'], inplace=True)
    return df.loc[index]


# +
# Info Table과 새로운 Info Table을 비교해서 추가될 값 출력
def makeup2(df):
    index1 = []
    index2 = []
    index3 = []
    j1 = df[df['_merge'] == 'right_only']
    j2 = df[df['_merge'] == 'left_only']
    i1 = j1.index
    i2 = j2.index
    for idx, val in enumerate(zip(df['useTime_x'].values, df['useTime_y'].values)):
        if val[0] != val[1]:
            index1.append(idx)
    for idx, val in enumerate(zip(df['limitYn_x'].values, df['limitYn_y'].values)):
        if val[0] != val[1]:
            index2.append(idx)
    for idx, val in enumerate(zip(df['delYn_x'].values, df['delYn_y'].values)):
        if val[0] != val[1]:
            index3.append(idx)
    set1 = set(index1)
    set2 = set(index2)
    set3 = set(index3)
    set4 = set(i1)
    set5 = set(i2)
    common_values = set1.union(set2, set3)
    common_values2 = set4.union(set5)
    final = common_values.difference(common_values2)
    index = list(final)
    df.drop(columns = ['useTime_x', 'limitYn_x', 'limitDetail_x', 'delYn_x', 'delDetail_x'], inplace=True)
    new_column_names = {}
    for k in df.columns[2:-1]:
        new_column_names[k] = k[:-2]
    df.rename(columns = new_column_names, inplace=True)
    df.drop(columns = ['_merge'], inplace=True)
    return df.loc[index]

# Info Table과 새로운 Info Table을 비교해서 추가될 값 출력 ver2
def makeup_status2(df):
    i = df[df['_merge'] == 'right_only']
    df.drop(columns = ['useTime_x', 'limitYn_x', 'limitDetail_x', 'delYn_x', 'delDetail_x'], inplace=True)
    new_column_names = {}
    for k in df.columns[2:-1]:
        new_column_names[k] = k[:-2]
    df.rename(columns = new_column_names, inplace=True)
    df.drop(columns = ['_merge'], inplace=True)
    return df.loc[i.index]


# -

# Meta Table과 새로운 Meta Table을 비교해서 추가될 값 출력
def no_makeup(df):
    i = df[df['_merge'] == 'right_only']
    df.drop(columns = ['statNm_x', 'chgerType_x', 'addr_x', 'location_x',
       'lat_x', 'lng_x', 'busiId_x', 'bnm_x', 'busiNm_x','busiCall_x', 
        'stat', 'statUpdDt', 'lastTsdt', 'lastTedt','nowTsdt', 'powerType_x', 'output_x', 'method_x', 'zcode_x',
       'zscode_x', 'kind_x', 'kindDetail_x', 'parkingFree_x', 'note_x','trafficYn_x'], inplace=True)
    new_column_names = {}
    for k in df.columns[2:-1]:
        new_column_names[k] = k[:-2]
    df.rename(columns = new_column_names, inplace=True)
    new_column_order = ['statNm', 'statId', 'chgerId', 'chgerType', 'addr', 'location', 'lat',
           'lng', 'busiId', 'bnm', 'busiNm', 'busiCall', 'powerType',
           'output', 'method', 'zcode', 'zscode', 'kind', 'kindDetail',
           'parkingFree', 'note', 'trafficYn']
    df = df[new_column_order]
    return df.loc[i.index]


#기존 Status Table 불러오기
def loadstatus():
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    up = pd.read_sql("select * from status", conn)
    
    conn.close()
    return up

# 기존 Info Table 불러오기
def loadinfo():
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    up = pd.read_sql("select * from info", conn)
    
    conn.close()
    return up


#기존 Meta Table 불러오기
def loadmeta():
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    no_up = pd.read_sql("select * from meta", conn)
    
    conn.close()
    return no_up

#기존 Log Table 불러오기
def loadlog():
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    no_up = pd.read_sql("select * from log3", conn)
    
    conn.close()
    return no_up


#갱신된 값에 대한 이전 값을 postgreSQL Log Table에 바로 추가
def inputlog(sub):
    sub.reset_index(drop=True, inplace=True)
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    cur=conn.cursor()
    for idx in tqdm(range(len(sub))):
        query = "insert into log3 (statid, chgerid, stat, statupddt, lasttsdt, lasttede, nowtsdt) values(%s, %s, %s, %s, %s, %s, %s)"
        data=tuple(sub.loc[idx])
        cur.execute(query,data)
        conn.commit()
    
    conn.close()

# +
#갱신된 값에 대한 이전 값을 postgreSQL Statue Table에 바로 추가 
def inputstatus1_1(up):
    up.reset_index(drop=True, inplace=True)
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    cur=conn.cursor()
    for idx in tqdm(range(len(up))):
        query = "insert into status (statid, chgerid, stat, statupddt, lasttsdt, lasttede, nowtsdt) values(%s, %s, %s, %s, %s, %s, %s)"
        data=tuple(up.loc[idx])
        cur.execute(query,data)
        conn.commit()
    
    conn.close()
    
def inputstatus1_2(up, conditions):
    up.reset_index(drop=True, inplace=True)
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    cur=conn.cursor()
    for condition in tqdm(conditions):
        column1_value, column2_value = condition
        delete_query = f"""
        DELETE FROM status
            WHERE statId = '{column1_value}'
              AND chgerId = '{column2_value}';
        """
        cur.execute(delete_query)
    
    conn.commit()
    cur.close()
    conn.close()
    
    up.reset_index(drop=True, inplace=True)
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    cur=conn.cursor()
    for idx in tqdm(range(len(up))):
        query = "insert into status (statid, chgerid, stat, statupddt, lasttsdt, lasttede, nowtsdt) values(%s, %s, %s, %s, %s, %s, %s)"
        data=tuple(up.loc[idx])
        cur.execute(query,data)
        conn.commit()

    conn.close()

# +
#갱신된 값에 대한 이전 값을 postgreSQL Info Table에 바로 추가 
def inputstatus2_1(up):
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    cur=conn.cursor()
    for idx in tqdm(range(len(up))):
        query = "insert into info (statid, chgerid, useTime, limitYn, limitDetail, delYn, delDetail) values(%s, %s, %s, %s, %s, %s, %s)"
        data=tuple(up.loc[idx])
        cur.execute(query,data)
        conn.commit()
    
    conn.close()
    
def inputstatus2_2(up, conditions):
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000

    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    cur=conn.cursor()
    for condition in tqdm(conditions):
        column1_value, column2_value = condition
        delete_query = f"""
        DELETE FROM info
            WHERE statId = '{column1_value}'
              AND chgerId = '{column2_value}';
        """
        cur.execute(delete_query)
    
    conn.commit()
    cur.close()
    
    cur=conn.cursor()
    for idx in tqdm(range(len(up))):
        query = "insert into info (statid, chgerid, useTime, limitYn, limitDetail, delYn, delDetail) values(%s, %s, %s, %s, %s, %s, %s)"
        data=tuple(up.loc[idx])
        cur.execute(query,data)
        conn.commit()

    
    conn.close()


# -

#갱신된 값만 Meta Table에 추가
def inputmeta(pre):
    pre.reset_index(drop=True, inplace=True)
    host = '****'  # Or the IP address of the Docker container
    dbname = '****'
    user = '****'   # Or your PostgreSQL username
    password = '****'
    port = 0000
    
    # Establish the database connection
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    
    cur=conn.cursor()
    for idx in tqdm(range(len(pre))):
        query = "insert into meta (statNm, statId, chgerId, chgerType, addr, location, lat, lng, busiId, bnm, busiNm, busiCall, powerType, output, method, zcode, zscode, kind, kindDetail, parkingFree, note, trafficYn) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        data=tuple(pre.loc[idx])
        cur.execute(query,data)
        conn.commit()
        
    
    conn.close()

temp = []
t = pd.read_parquet('./charging_station_every_5/charging_station_2024-01-14_08-29.parquet')
temp.append(t)

# +
first_execution = False
# temp = []
length = 0
def collection():
    global first_execution
    global temp
    global length
    url = 'http://apis.data.go.kr/B552584/EvCharger/getChargerInfo'
    combined_df = pd.DataFrame()
    
    for i in range(1, 26):
        params ={'serviceKey' : 'uCAkdprbr14iZN6Qlv+6+VM4msmCjfezBSjpKy8g2Cb3X3GiZtHAWvc452LfJcbtDHmPxdT0mtRfNfXH1hntcg==', 'pageNo' : f'{i}', 'numOfRows' : '10000'}
        retries = 5  # Number of retries before giving up
        while retries > 0:
            try:
                response = requests.get(url, params=params)
                json_data = xml_to_json(response.content)
                df = pd.json_normalize(json_data['response']['body']['items']['item'])
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                break  # Break out of the retry loop if successful
            except Exception as e:
                print(f"Error fetching data (retrying): {e}")
                print(f"Error happens at: {datetime.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d_%H-%M')}")
                retries -= 1
                time.sleep(5)  # Wait for a moment before retrying
    
#     current_time = datetime.datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d_%H-%M')
#     filename = f'charging_station_{current_time}.parquet'
#     copy_df = copy.deepcopy(combined_df)
#     copy_df.to_parquet('./chargingstation/' + filename, engine='pyarrow')
    time.sleep(5)
    temp.append(combined_df)
    print(len(temp))
    up = loadstatus()
    up2 = loadinfo()
    no_up = loadmeta()
    length = len(no_up)
    up.columns = ['statId', 'chgerId', 'stat', 'statUpdDt', 'lastTsdt', 'lastTedt', 'nowTsdt']
    up2.columns = ['statId', 'chgerId','useTime', 'limitYn', 'limitDetail', 'delYn', 'delDetail']
    no_up.columns = ['statNm', 'statId', 'chgerId', 'chgerType', 'addr', 'location', 'lat',
       'lng', 'busiId', 'bnm', 'busiNm', 'busiCall', 'powerType', 'output',
       'method', 'zcode', 'zscode', 'kind', 'kindDetail', 'parkingFree',
       'note', 'trafficYn']
    if first_execution:
        db = distribute(combined_df)
        up = pd.concat([up, db[0]], ignore_index = True)
        up2 = pd.concat([up2, db[1]], ignore_index= True)
        no_up = pd.concat([no_up, db[2]], ignore_index = True)
        inputmeta(no_up)
        inputstatus1_1(up)
        inputstatus2_1(up2)
        first_execution = False
    else:
        # meta
        db = no_up.merge(temp[1], how = 'outer', left_on = ['statId', 'chgerId'], right_on = ['statId', 'chgerId'], indicator = True)
        no_sub = no_makeup(db)
        no_up = pd.concat([no_up, no_sub], ignore_index = True)
        pre = no_up[length:]
        inputmeta(pre)
        # status 1, 2 & log
        db = temp[0].merge(temp[1], how = 'outer', left_on = ['statId', 'chgerId'], right_on = ['statId', 'chgerId'], indicator = True)
        sub = makeup(db)
        sub.drop_duplicates(inplace=True)
        sub['stat'].fillna('0', inplace=True)
        inputlog(sub)
        
        tem = distribute(temp[1])
        db2 = up.merge(tem[0], how = 'outer', left_on = ['statId', 'chgerId'], right_on = ['statId', 'chgerId'], indicator = True)
        sub2 = makeup_status(db2)
        sub2.drop_duplicates(inplace=True)
        up = pd.concat([up, sub2], ignore_index = True)
        duplicates = up[up.duplicated(subset=['statId', 'chgerId'], keep=False)]
        up.drop_duplicates(subset=['statId', 'chgerId'], keep='last',inplace=True)
        up['stat'].fillna('0', inplace=True)
        duplicates.drop_duplicates(subset=['statId', 'chgerId'], inplace=True)
        conditions = []
        for val in zip(duplicates['statId'].values, duplicates['chgerId'].values):
            conditions.append(val)
        inputstatus1_2(up[len(up)-len(sub2):], conditions)
        
        db3 = up2.merge(tem[1], how = 'outer', left_on = ['statId', 'chgerId'], right_on = ['statId', 'chgerId'], indicator = True)
        sub3 = makeup2(db3)
        sub3.drop_duplicates(inplace=True)
        up2 = pd.concat([up2, sub3], ignore_index = True)
        duplicates2 = up2[up2.duplicated(subset=['statId', 'chgerId'], keep=False)]
        up2.drop_duplicates(subset=['statId', 'chgerId'], keep='last',inplace=True)
        duplicates2.drop_duplicates(subset=['statId', 'chgerId'], inplace=True)
        conditions2 = []
        for val in zip(duplicates2['statId'].values, duplicates2['chgerId'].values):
            conditions2.append(val)
        inputstatus2_2(up2[len(up2):], conditions2)
        db3 = up2.merge(tem[1], how = 'outer', left_on = ['statId', 'chgerId'], right_on = ['statId', 'chgerId'], indicator = True)
        sub3 = makeup_status2(db3)
        sub3.drop_duplicates(inplace=True)
        up2 = pd.concat([up2, sub3], ignore_index = True)
        inputstatus2_1(sub3)
        temp.pop(0)


# -

schedule.every(5).minutes.do(collection)

while True:
    schedule.run_pending()
    time.sleep(1)
