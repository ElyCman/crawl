import pandas as pd
import numpy as np
import pymongo
import warnings
import re

warnings.filterwarnings('ignore')

client = pymongo.MongoClient('localhost', 27017)
db = client['51job']
table = db['DataAnalyst']
df = pd.DataFrame(list(table.find()))

# 数据前期准备
def dfunc(data):
    data.drop(['_id', 'url'], axis = 1, inplace = True)
    data.drop_duplicates(inplace = True)
    # 重置索引
    data.reset_index(inplace = True)
    data.drop(['index'], axis = 1, inplace = True)
    return data

# 地区
def area(data):
    area = data['area'].str.split('-', expand = True)
    # 重命名
    area.rename(columns = {0: 'city', 1: 'district'}, inplace = True)
    area['city'] = area['city'].apply(func)
    area['district'] = area['district'].apply(func)
    return area

# 定义func函数
def func(x):
    if len(str(x)) == 0:
        return np.nan
    elif str(x) == 'None' or str(x) == '异地招聘':
        return np.nan
    else:
        return x.strip()

# 发布时间
def pub(data):
    pub = data['publish_date'].str.split('-', expand = True)
    pub.rename(columns = {0: 'month', 1: 'day'}, inplace = True)

    pub['month'] = pub['month'].apply(func)
    pub['day'] = pub['day'].apply(func)
    pub['season'] = pub['month'].apply(season)
    pub['ten_day'] = pub['day'].apply(ten_day)
    return pub

# 定义season、ten_day函数
def season(x):
    if int(x) >= 1 and int(x) < 4:
        return '第一季度'
    elif int(x) >=4 and int(x) < 7:
        return '第二季度'
    elif int(x) >= 7 and int(x) < 10:
        return '第三季度'
    else:
        return '第四季度'

def ten_day(x):
    if str(x) == 'None':
        return np.nan
    elif int(x) >= 1 and int(x) <= 10:
        return '上旬'
    elif int(x) > 10 and int(x) <= 20:
        return '中旬'
    else:
        return '下旬'

# 工资
def salary(data):
    sal = data['salary'].apply(get_sal)
    data['sal_low'] = sal.str[0]
    data['sal_high'] = sal.str[1]
    data['sal_avg'] = data[['sal_low', 'sal_high']].mean(axis = 1)
    return data

# 定义get_sal函数
def get_sal(x):
    t = x.strip()
    if '元/天' in t:
        tem = [float(i) for i in re.findall('[0-9]+\.?[0-9]*', t)]
        sal = [tem[0], tem[0]]
    elif '万/月' in t:
        sal = [float(i) * 10000 for i in re.findall('[0-9]+\.?[0-9]*', t)]
    elif '千/月' in t:
        sal = [float(i) * 1000 for i in re.findall('[0-9]+\.?[0-9]*', t)]
    elif '万/年' in t:
        sal = [float(i) * 10000 / 12 for i in re.findall('[0-9]+\.?[0-9]*', t)]
    else:
        sal = [np.nan, np.nan]
    return sal

# 学历、行业
def oth(degree, industry):
    degree['degree'] = degree['degree'].apply(lambda x: '未说明' if len(str(x)) == 0 else x)

    indr = industry['industry'].apply(lambda x: x.replace(',', '/').split('/')[0])
    indr = pd.DataFrame(indr)

    data = pd.concat([degree, indr], axis = 1)
    return data

if __name__ == '__main__':
    df = dfunc(df)
    area1 = df[['area']]
    area = area(area1)
    publish = df[['publish_date']]
    pub = pub(publish)
    sal = df[['salary']]
    salary = salary(sal)
    deg_indr = oth(df[['degree']], df[['industry']])
    final = pd.concat([area, pub, salary[['sal_low', 'sal_high', 'sal_avg']],deg_indr, df[['work_of_years', 'business_nature', 'people']]], axis = 1)

