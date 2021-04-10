#coding=UTF-8
from Configure import *
#选出前三个相似的
def get_topk_likeness_date(inference_series, likeness_stack, series_stack, top_k=3, algori='mean',increase_index=1):
    # top_k=3 选3个相似日
    # algori='mean'  weighted_mean, 算术平均/加权平均
    
    #argpartition第二个参数k-th是指将比 第k小的数(X_k) 的数的索引放在前面，
    #比X_k大的数的索引按值放在后面，k-th前后两部分的索引不一定是准确的，
    #argsort是所有值排序得到索引，argpartition多了第二个参数k-th可能注重排k-th，比全排序要少一些计算，效率高一点
    


    likeness_stack_topk_index = np.argpartition(likeness_stack,-top_k,axis=2)[:,:,-top_k:] 
    x = [ ]
    for j in  likeness_stack_topk_index:
        for p,q in zip(series_stack,j):
            x.append(p[q])
    series_stack_topk = np.array(x).reshape(likeness_stack.shape[0],likeness_stack.shape[1],top_k)
    
    #算术平均法
    if algori=='mean':
        series_stack_topk_mean = np.round(np.mean(series_stack_topk,axis=2), decimals=3)*increase_index
    
    #按相似度加权平均
    elif algori=='weighted_mean':
        likeness_stack_topk =  ndims_app(likeness_stack,likeness_stack_topk_index)
        series_stack_topk_mean = ndims_app(series_stack_topk,likeness_stack_topk,func2)*increase_index
    
    resid = np.round(np.abs(series_stack_topk_mean - inference_series),decimals=3)
    resid_percent = np.round(np.abs(series_stack_topk_mean - inference_series)/inference_series,decimals=3)
    return likeness_stack_topk_index,resid,resid_percent



def func1(p,q):
    return p[q]
def func2(p,q):
    return np.round(np.sum(p*q)/np.sum(q), decimals=3)

#训练用
def ndims_app(a,b,func=func1):
    x =[]
    for i,j in zip(a,b):
        for p,q in zip(i,j):
            tmp = func(p,q)
            x.append(tmp)
    if type(tmp)==np.ndarray:
        x = np.array(x).reshape(b.shape[0],b.shape[1],len(tmp))
    else:
        x = np.array(x).reshape(b.shape[0],b.shape[1])
    return x


#预测用
def ndims_app1(a,b,func=func1):
    x =[]
    for i,j in zip(a,b):
        tmp = func(i,j)
        x.append(tmp)
    if type(tmp)==np.ndarray:
        x = np.array(x).reshape(b.shape[0],len(tmp))
    else:
        x = np.array(x).reshape(b.shape[0])
    return x


def get_topk_likeness_mean(compare_date,likeness_stack, series_stack,top_k=3, algori='mean',increase_index=1):
    
    #argpartition第二个参数k-th是指将比 第k小的数(X_k) 的数的索引放在前面，
    #比X_k大的数的索引按值从小到大放在后面，argsort是所有值排序得到索引，
    #argpartition多了第二个参数k-th可能注重排Top-k，比全排序要少一些计算，效率高一点
    likeness_stack_topk_index = np.argpartition(likeness_stack,-top_k,axis=1)[:,-top_k:] 

    series_stack_topk = ndims_app1(series_stack,likeness_stack_topk_index)
    
    #算术平均法
    if algori=='mean':
        series_stack_topk_mean = (np.mean(series_stack_topk,axis=1)*increase_index).astype(int)
    
    #按相似度加权平均
    elif algori=='weighted_mean':
        likeness_stack_topk = ndims_app1(likeness_stack,likeness_stack_topk_index)
        series_stack_topk_mean = (ndims_app1(series_stack_topk,likeness_stack_topk,func2)*increase_index).astype(int)
        print(likeness_stack_topk)
    
    
    likeness_topk_day_list = []
    for index in likeness_stack_topk_index :
        likeness_topk_day_list.append(compare_date[index].values.tolist())
    print("Top-k likeness  day for each time period :\n",likeness_topk_day_list)
    print("\nTop-k likeness  mean_value  for each time period :\n",series_stack_topk_mean)
    
    return likeness_stack_topk_index,series_stack_topk_mean,likeness_topk_day_list


