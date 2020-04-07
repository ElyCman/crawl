import pymongo
import pandas as pd
import wordcloud
import jieba
import numpy as np
import matplotlib.pyplot as plt

from wordcloud import WordCloud

# 导入弹幕数据
client = pymongo.MongoClient('localhost', 27017)
db = client['bilibili']
table = db['danmu']
bi = pd.DataFrame(list(table.find()))

df = bi[['danmu']]
df.drop(df.loc[df['danmu'].str.len() == 0].index, inplace = True)
df1 = df
cont = df1['danmu'].values.tolist()

# 导入停用词
stopwd = pd.read_table("stopwords.txt", sep = '\n',
                      encoding = 'utf-8', names = ['stop'])
stoplt = stopwd['stop'].values.tolist()

# 把弹幕内容列表进行分词
words = []
for line in cont:
    seg = jieba.lcut(line)
    for word in seg:
        if word in ('<', '=', '>') or len(word) <= 1:
            continue
        elif word in stoplt:
            continue
        else:
            words.append(word)

# 计算词频
df2 = pd.DataFrame({'seg_word': words})
word_count = df2.groupby(by = ['seg_word'])['seg_word'].agg({np.size})
word_count.reset_index(inplace = True)
word_count.rename(columns = {'size': 'counts'}, inplace = True)
word_count.sort_values(by = ['counts'], ascending = False, inplace = True)

# 词频字典
word_freq = {x[0] : x[1] for x in word_count.values}

# 设置画图参数
plt.rcParams['figure.figsize'] = (12, 8)
my_wordcloud_2 = WordCloud(font_path = r'C:\Windows\Fonts\STXINGKA.TTF',
                           width = 800,
                           height = 400,
                           max_words = 300, # 显示词的个数
                           background_color = 'white')
pic_cloud = my_wordcloud_2.fit_words(word_freq)

# 画图
plt.imshow(pic_cloud, interpolation = 'bilinear') # 图片清晰度
plt.axis('off')
plt.show();
