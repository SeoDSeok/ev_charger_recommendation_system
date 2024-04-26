import pickle, pickle5
import pandas as pd
import numpy as np
from tqdm import tqdm
with open('../Data/evdriver/preprocessed/meta.pickle', 'rb') as f:
    meta = pickle.load(f)

with open('../Data/evdriver/preprocessed/drivers.pickle', 'rb') as f:
    train_dv = pickle.load(f)

with open('../Data/evdriver/preprocessed/testset_df.pickle', 'rb') as f:
    test_dv = pickle.load(f)

test_dv = test_dv.sort_values(by=['Driver']).reset_index(drop=True)

entire_rank = np.load('../Data/evdriver/entire_rank.npy')

def evaluation(result, driver_id, test_dv, meta, criteria):
    location = test_dv.loc[test_dv['Driver'] == driver_id][criteria].values[0]
    teststat = test_dv.loc[test_dv['Driver'] == driver_id]['statid'].values[0]
    speed = test_dv.loc[test_dv['Driver'] == driver_id]['speed'].values
    if len(speed) == 1:
        flag = True
    else:
        flag = False

    driver_pref = result[driver_id]
    #[1011, 14000, 13000] -> statid의 list
    driver_rank = driver_pref.argsort()[::-1].astype(str)
    #하나의 Cluster로 위치 필터링
    driver_meta = meta[meta[criteria] == location]
    recommended_df = pd.DataFrame([])

    for i in range(len(driver_rank)):
        if len(recommended_df) > 19:
            false_count = len(recommended_df) - recommended_df.duplicated('statid').sum()
            if false_count == 20:
                break
        #추천된 statid가 Cluster에 속하면,
        if driver_rank[i] in driver_meta['statid'].values:
            stat_driver = driver_meta.loc[driver_meta['statid'] == driver_rank[i]]
            #추천된 statid의 speed가 testset의 speed와 같으면,
            if flag:
                if stat_driver['speed'].values[0] == speed:
                    recommended_df = pd.concat([recommended_df, stat_driver])
            else:
                recommended_df = pd.concat([recommended_df, stat_driver])
                
    if flag:
        recommended_df = recommended_df.loc[recommended_df['speed'] == speed[0]].reset_index(drop=True)
    else:
        recommended_df = recommended_df.drop_duplicates('statid').reset_index(drop=True)

    try:
        rank = recommended_df.loc[recommended_df['statid'] == teststat].index[0] 
        rank = rank + 1
    except:
        rank = 0
    return recommended_df, rank, len(recommended_df)
    

# driver_num = len(test_dv['Driver'].unique())

# rank = []
# for i in tqdm(range(driver_num)):
#     a, b, c = evaluation(entire_rank, i, test_dv, meta, 'zscode')
#     rank.append((b,c))
#     if i == 100000:
#         np.save('./check!!!!!!.npy', np.array(rank))

# rank = np.array(rank)
# np.save('./rank_zscode_20.npy', rank)

def get_ndcg(recommended_df, test_dv, driver_id, K):
    busiid = test_dv.loc[test_dv['Driver'] == driver_id]['busiid'].values[0]
    ex = (recommended_df['busiid'] == busiid).values
    
    ndcgs = []
    for j in K:
        if len(ex) >= j:
            dcg = 0
            for i in range(j):
                if ex[i] == True:
                    dcg += 1/np.log2(2 + i)

            idcg = 0
            sum_ex = ex.sum()
            if sum_ex >= j:
                for i in range(j):
                    idcg += 1/np.log2(2 + i)
            else:
                for i in range(sum_ex):
                    idcg += 1/np.log2(2 + i)

            if idcg == 0:
                ndcgs.append(0)
            else:
                ndcgs.append(dcg/idcg)
        else:
            ndcgs.append(ndcgs[-1])

    return ndcgs



ndcgs = []
for i in tqdm(range(10000)):
    a, b, c = evaluation(entire_rank, i, test_dv, meta, 'zscode')
    try:
        ndcg = get_ndcg(a, test_dv, i, [1, 5, 10, 15, 20])
        ndcgs.append(ndcg)
    except:
        print(i)

ndcgs = np.array(ndcgs)
np.save('./ndcgs_zscode_10000.npy', ndcgs)

ndcgs1 = []
for i in tqdm(range(10000)):
    a, b, c = evaluation(entire_rank, i, test_dv, meta, 'Cluster')
    try:
        ndcg = get_ndcg(a, test_dv, i, [1, 5, 10, 15 ,20])
        ndcgs1.append(ndcg)
    except:
        print(i)

ndcgs1 = np.array(ndcgs1)
np.save('./ndcgs_cluster_10000.npy', ndcgs1)