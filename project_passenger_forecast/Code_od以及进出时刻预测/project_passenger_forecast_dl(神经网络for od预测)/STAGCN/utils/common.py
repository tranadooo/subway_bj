from sklearn.decomposition import PCA 
def get_ncomponents(x, var_percent=0.95):
    pca = PCA(n_components=var_percent)
    pca.fit(x)
#     print(pca.explained_variance_ratio_)
#     print(pca.explained_variance_)
    print(pca.n_components_)
    return pca.n_components_

from  datetime import datetime
import datetime as dt
def date_plus(date, plus_n):
    #date  str
    #plus_n int
    #return : str
    return (datetime.strptime(date,'%Y%m%d').date()+dt.timedelta(days=plus_n)).strftime("%Y%m%d")

def get_time():
    nowTime=dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return nowTime