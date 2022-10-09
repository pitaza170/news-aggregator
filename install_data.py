import dagster as dg
import numpy as np
import requests
from lxml import html
from bs4 import BeautifulSoup
import pandas as pd
import re


def striphtml(data):
    p = re.compile(r'<.*?>')
    return p.sub('', data)


links = []
names_articles = []
tags_articles = []
text = []
dates = []

for i in range(1, 100):
    # print(i)
    url = "http://www.consultant.ru/legalnews/?page=" + str(i)
    r = requests.get(url)

    soup = BeautifulSoup(r.text, features='lxml')
    list_articles = soup.findAll('a', {'class': 'listing-news__item-title'})
    # print("length articles=", len(list_articles))
    # print(list_articles[0])
    # print(list_articles[0].get('href'))
    # print(list_articles[0].text)

    current_links = []
    current_named_articles = []

    for article in list_articles:
        links.append(''.join(['http://www.consultant.ru/', article.get('href')]))
        current_links.append(''.join(['http://www.consultant.ru/', article.get('href')]))
        names_articles.append(article.text)

    for link in current_links:
        r = requests.get(link)

        soup = BeautifulSoup(r.text, features='lxml')
        date = soup.find("div", {"class": "news-page__date"})

        soup = BeautifulSoup(r.text, features='lxml')
        tags = soup.findAll("span", {"class": "tags-news__text"})
        tags_list = []
        for tag in tags:
            tags_list.append(tag.text)
        tags_articles.append('|'.join(tags_list))

        page_text = soup.find("div", {"class": "news-page__text"})
        p_tags = page_text.findAll('p')
        full_text = []
        for p in p_tags:
            full_text.append(p.text)
        text.append(''.join(full_text))

        day, month, year = date.text.split(" ")

        if month == 'января':
            month = 1
        elif month == 'февраля':
            month = 2
        elif month == 'марта':
            month = 3
        elif month == 'апреля':
            month = 4
        elif month == 'мая':
            month = 5
        elif month == 'июня':
            month = 6
        elif month == 'июля':
            month = 7
        elif month == 'августа':
            month = 8
        elif month == 'сентября':
            month = 9
        elif month == 'октября':
            month = 10
        elif month == 'ноября':
            month = 11
        elif month == 'декабря':
            month = 12

        dates.append('-'.join([str(year), str(month), str(day)]))

    # print("length tags", len(tags_articles))
#
# print("length text=", len(text))
# print("length links=", len(links))
# print("length tags_articles", len(tags_articles))
# print("named_articles", len(names_articles))


data = {
    "Title": names_articles,
    "Description": text,
    "Links": links,
    "Publication Date": dates,
}

df1 = pd.DataFrame(data)
df1.to_csv("test.csv")

import feedparser
import csv
import pandas as pd
import re

newsurls = {'Kommersant': 'https://www.kommersant.ru/RSS/news.xml',
'Lenta.ru': 'https://lenta.ru/rss/',
'Vesti': 'https://www.vesti.ru/vesti.rss',
            'vc.ru': 'https://vc.ru/rss/all',
            'tass.ru': 'https://tass.ru/rss/v2.xml',
            "habr.ru": 'https://habr.com/ru/rss/all/all/',
            'ria.ru': 'https://ria.ru/export/rss2/index.xml?page_type=google_newsstand',
            'vm.ru': 'https://vm.ru/rss',
            'knife.media': 'https://knife.media/feed/',
            'https://journal.tinkoff.ru' :'https://journal.tinkoff.ru/feed/',
            'rb.ru': 'https://rb.ru/feeds/all/',
            'cossa.ru': 'https://www.cossa.ru/rss/',
            'snob.ru': 'https://snob.ru/rss/',
            'news.mail.ru': 'https://news.mail.ru/rss',
            'fontanka.ru': 'https://www.fontanka.ru/fontanka.rss',
            #'aif.ru': 'https://aif.ru/rss/news.php'
            } #пример словаря RSS-лент
                                           #русскоязычных источников

f_all_news = 'news.csv'
f_certain_news = 'certainnews23march.csv'


def parseRSS(rss_url):  # функция получает линк на рсс ленту, возвращает распаршенную ленту с помощью feedpaeser
    return feedparser.parse(rss_url)


def getHeadlines(rss_url):  # функция для получения заголовков новости
    headlines = []
    feed = parseRSS(rss_url)
    for newsitem in feed['items']:
        headlines.append(newsitem['title'])
    return headlines


def getDescriptions(rss_url):  # функция для получения описания новости
    descriptions = []
    feed = parseRSS(rss_url)
    for newsitem in feed['items']:
        descriptions.append(newsitem['description'])
    return descriptions


def getLinks(rss_url):  # функция для получения ссылки на источник новости
    links = []
    feed = parseRSS(rss_url)
    for newsitem in feed['items']:
        links.append(newsitem['link'])
    return links


def getDates(rss_url):  # функция для получения даты публикации новости
    dates = []
    feed = parseRSS(rss_url)
    for newsitem in feed['items']:
        dates.append(newsitem['published'])
    return dates


allheadlines = []
alldescriptions = []
alllinks = []
alldates = []
# Прогоняем нашии URL и добавляем их в наши пустые списки
for key, url in newsurls.items():
    allheadlines.extend(getHeadlines(url))

for key, url in newsurls.items():
    alldescriptions.extend(getDescriptions(url))

for key, url in newsurls.items():
    alllinks.extend(getLinks(url))

for key, url in newsurls.items():
    alldates.extend(getDates(url))


def write_all_news(all_news_filepath):  # функция для записи всех новостей в .csv, возвращает нам этот датасет
    header = ['Title', 'Description', 'Links', 'Publication Date']

    with open(all_news_filepath, 'w', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')

        writer.writerow(i for i in header)

        for a, b, c, d in zip(allheadlines, alldescriptions,
                              alllinks, alldates):
            writer.writerow((a, b, c, d))

        df = pd.read_csv(all_news_filepath)

    return df


def looking_for_certain_news(all_news_filepath, certain_news_filepath, target1,
                             target2):  # функция для поиска, а затем записи
    # определенных новостей по таргета,
    # затем возвращает этот датасет
    df = pd.read_csv(all_news_filepath)

    result = df.apply(lambda x: x.str.contains(target1, na=False,
                                               flags=re.IGNORECASE, regex=True)).any(axis=1)
    result2 = df.apply(lambda x: x.str.contains(target2, na=False,
                                                flags=re.IGNORECASE, regex=True)).any(axis=1)
    new_df = df[result & result2]

    new_df.to_csv(certain_news_filepath
                  , sep='\t', encoding='utf-8-sig')

    return new_df

write_all_news(f_all_news)
df2 = pd.read_csv(f_all_news)

news = pd.concat([df1, df2])
#news.to_csv("data.csv")

#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import warnings 
sns.set_style('darkgrid')
warnings.filterwarnings('ignore')
import re

from nltk.corpus import stopwords
from tqdm.auto import tqdm
tqdm.pandas()
import pymorphy2

m = pymorphy2.MorphAnalyzer()
import nltk

nltk.download('stopwords')
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim import similarities
from gensim.models import lsimodel
import math
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from sklearn.feature_extraction.text import TfidfVectorizer


# In[2]:


from pandas import datetime


# In[3]:


get_ipython().system(' pip install pymorphy2')


# In[4]:


#Теги
accountant = r'(бухгалтер) | (бухгалтер) | (бухгалтерия) | (бух.*?учет) | (налог) | (налоги) | (налоговая) |(ЕГАИС) | (кадровый учет) | (ФСБУ 25/2018) | (НДС) | (инспекция) | (аккредитация) | (новый) |(изменения) | (изменение) | (ИП) | (ООО) | (деньги) | (денег) | (НДФЛ) | (ЭДО) | (банкрот) | (банкротство) |(счет) | (акт) | (оплата) | (зарплата) | (налогообложение) | (сделка) | (отчет) | (отчёт) | (долг) | (аванс) | (УПД) |(лицензия) | (работодатель) | (документы) | (справка) | (минфин) | (министерство финансов) | (МРОТ) | (выручка)|(НДС) | (накладная) | (страховка)|(кредит) | (кредитование) | (физлицо) | (юрлицо) | (финансы) | (финансирование) | (имущество) |(статформа) | (статформу) | (статформы) | (недвижимость) | (расходы) | (расходов) | (расходах)'  
              
business = r'(бизнес) | (бизнес) | (менеджмент) | (тайм-менеджмент) | (управление)|(управленец) | (новый) | (изменения) | (ИП) | (ООО) | (деньги) | (денег) | (самозанятый)|(самозанятость) | (банкрот) | (банкротство) | (товар) | (бизнес.*?стратегия) | (бизнес.*?идея)|(сделка) | (выручка) | (реклама) | (продукт) | (MVP) | (продажи) | (планирование) | (клиент) | (клиенты)|(конкурент) | (конкуренция) | (кризис) | (экспорт) | (импорт) | (рынок) | (доход) | (убыток) | (поставки)|(продажи) | (производитель) | (производство рост) | (снижение) | (спрос) | (предложение)|(мылый.*?бизнес) | (средний.*?бизнес) | (кредит) | (кредитование) | (физлицо) | (юрлицо) | (бизнеса)|(бизнесом) | (бизнесу) | (бизнесе) | (финансы) | (финансирование) | (расходы) | (расходов) | (расходах)'


# In[5]:


print(business)


# In[6]:


#Преобразование слов в именительный падеж
mystopwords = stopwords.words('russian') + [
    'это', 'наш' , 'тыс', 'млн', 'млрд', 'также',  'т', 'д',
    'который','прошлый','сей', 'свой', 'наш', 'мочь', 'такой'
]
ru_words = re.compile("[А-Яа-я]+")


def words_only(text):
    return " ".join(ru_words.findall(text))


def lemmatize(text):
    try:
        return  " ".join([m.parse(w)[0].normal_form for w in text.lower().split()])
    except:
        return " "


def remove_stopwords(text, mystopwords = mystopwords):
    try:
        return " ".join([token for token in text.split() if not token in mystopwords])
    except:
        return ""

    
def preprocess(text):
    return remove_stopwords(lemmatize(words_only(text.lower())))


# In[7]:


class DataPreprocessor:
    def __init__(self):
        
        self.tags_find = None
        
        self.name_tags = None
        
        self.count_tags = None
        
        self.cl = 0
        
    def prime_fit(self, df, profession): 
        
        # Подсчет количества профессий, с возможностью расширения ролей 
        
        self.cl += 1 
        
        self.tags_find = df["union_im"].apply(lambda x: re.findall(profession, str(x).lower()))
        
        self.name_tags = 'tags_profession' + str(self.cl)

        self.count_tags = 'count_profession'+ str(self.cl)
        
#     def fit(self, df, profession, count): 
#         #Поиск тегов
        
#         self.tags_find = df["union_im"].apply(lambda x: re.findall(profession, str(x).lower()))
        
#         self.name_tags = 'tags_profession' + str(count)

#         self.count_tags = 'count_profession'+ str(count)
        
    def transform(self, df, profession):
        
        #Очистка тегов
        ans = []
        for line in (self.tags_find):
            buf = []
            if len(line):
                buf1 = ""
                for j in range(len(line)):    
                    el = set(line[j])
                    el.remove('')
                    element = str(el.pop())
                    if (element not in buf1):
                        buf1 += element + ' '         
                ans.append(buf1)
            else:
                ans.append(None)
                
        
        df[self.name_tags] = ans
       
        #Подсчет тегов
        
        df[self.count_tags] = 0
        for i, line in enumerate(df[self.name_tags]):
            if line != None:
                df[self.count_tags].iloc[i] = len(line.split())
            else:
                df[self.count_tags].iloc[i] = 0
                
        # Добавление нового датафрема к основному
        
        
        return df
    
#     def new_data_loading(self, path_data):
        
#         # загрузка нового дата фрема
        
#         df = pd.read_csv(path_data)
        
#         df.rename(columns = {"Title": "title", "Description": "text", "Links": "links", "Publication Date": "date"}, inplace = True)
        
#         # Обработка даты

#         df['date'] = df['date'].apply(lambda x: pd.Timestamp(x)) 
        
#         df = df.loc[df["text"].isna() == False]

        
#         # Приведение текста в именительный падеж
        
#         df["text_im"] = df["text"].astype('str').progress_apply(preprocess)
        
#         df["title_im"] = df["title"].astype('str').progress_apply(preprocess)
        
#         # объединение текста
        
#         df["union_im"] = df["text_im"] + df["title_im"]
        
#         # удаление ненужного
#         df.drop("text_im", axis = 1, inplace = True)
        
#         df.drop("title_im", axis = 1, inplace = True)
        
#         # Удаление пустых строк
#         df = df.loc[df["text"].isna() == False]
        
#         return df

    def prime_data_loading(self, path_data):
        
        prime_news = pd.read_csv(path_data)
        
        # Обработка даты
        prime_news['date'] = prime_news['date'].apply(lambda x: pd.Timestamp(x))
        
        
        # Удаление ненужного столбца 
        prime_news.drop("tags", axis = 1, inplace = True)
        
        # Приведение текста в именительный падеж
        prime_news["text_im"] = prime_news["text"].astype('str').progress_apply(preprocess)
        prime_news["title_im"] = prime_news["title"].astype('str').progress_apply(preprocess)
        
         # объединение текста
        prime_news["union_im"] = prime_news["text_im"] + prime_news["title_im"]
        
        # удаление ненужного
        prime_news.drop("text_im", axis = 1, inplace = True)
        
        prime_news.drop("title_im", axis = 1, inplace = True)
        prime_news.drop("Unnamed: 0", axis = 1, inplace = True)
        
        return prime_news
    
    def data_concat(self, prime_df, df):      
        
        #Добавление нового датафрема к основному
        prime_df = pd.concat([prime_df, df], axis=0, ignore_index=True)
        
        return prime_df
    
    def more_impotant_news(self, df, count_profession):
        try:
            return( df[df[count_profession] >= 4].sort_values(["date", count_profession], ascending = False, inplace = False))
        except:
            return( df[df[count_profession] >= 4].sort_values([count_profession], ascending = False, inplace = False))
        
        
        


# In[8]:



#Создание объекта класса

preprocessor = DataPreprocessor()

# Создание основного дата сета
prime_news = preprocessor.prime_data_loading("TestwithData.csv")
#Добавление профессии бухгалтера, #Поиск тегов
preprocessor.prime_fit(prime_news, accountant)
#Очистка, кол.тегов
preprocessor.transform(prime_news, accountant)
#Добавление профессии бизнесменб #Поиск тегов
preprocessor.prime_fit(prime_news, business)
#Очистка, кол.тегов
preprocessor.transform(prime_news, business)


# In[9]:


# new_news = preprocessor.new_data_loading("new_news_dataset.csv")
# #Поиск тегов
# preprocessor.fit(new_news, accountant, 1)
# #Очистка кол.тегов
# preprocessor.transform(new_news, accountant)
# #Добавление профессии бизнесмен
# preprocessor.fit(new_news, business, 2)
# #Поиск тегов
# preprocessor.transform(new_news, business)


# In[10]:


# #Добавление нового датафрейма к основному
# prime_news = preprocessor.data_concat(prime_news, new_news)


# In[11]:


# Самые важные и свежие новости для бухгалтера
news_for_accountant = preprocessor.more_impotant_news(prime_news, "count_profession1")
news_for_accountant.drop(["tags_profession2", "count_profession2"], axis = 1, inplace = True)
news_for_accountant


# In[12]:


# Самые важные и свежие новости для бизнеса
news_for_business = preprocessor.more_impotant_news(prime_news, "count_profession2")
news_for_business.drop(["tags_profession1", "count_profession1"], axis = 1, inplace = True)
news_for_business


# In[13]:


def tagged_document(list_of_ListOfWords):
    for x, ListOfWords in enumerate(list_of_ListOfWords):
        yield TaggedDocument(ListOfWords, [x])
data_train = list(tagged_document(prime_news.text))

d2v_model = Doc2Vec(vector_size=100, min_count=2, epochs=50)
d2v_model.build_vocab(data_train)
d2v_model.train(data_train, total_examples=d2v_model.corpus_count, epochs=d2v_model.epochs)


# In[14]:


def dubles(df, model):
        doubles = []
        def distCosine (vecA, vecB):
            def dotProduct (vecA, vecB):
                d = 0.0
                for i in range(len(vecA)):
                    d += vecA[i] * vecB[i]
                return d
            return dotProduct (vecA,vecB) / math.sqrt(dotProduct(vecA,vecA)) / math.sqrt(dotProduct(vecB,vecB))
        lens = 50
        eps = 0.0005
        while lens > 10:
            corp = df.iloc[0:50]
            corp = corp.reset_index()
            for i in range(len(corp) - 1, -1, -1):
                emb0 = model.infer_vector(list(corp.iloc[i].text.split()))
                for j in range(i - 1, -1, -1):
                    emb1 = model.infer_vector(list(corp.iloc[j].text.split()))
                    if abs(distCosine(emb0, emb1)) < eps:
                        doubles.append([corp.iloc[i].links, corp.iloc[j].links])
                        corp = corp.iloc[0 : i]
                        break
            lens = len(corp)
            eps += 0.0003
        return [corp, doubles]


# In[15]:


news_for_accountant = dubles(news_for_accountant,d2v_model)[0]
news_for_business = dubles(news_for_business,d2v_model)[0]


# In[16]:


def topics(df):
    ans = []
    tfidf = TfidfVectorizer(analyzer='word', ngram_range=(1,2), min_df = 0)
    tfidf_matrix =  tfidf.fit_transform(df.text)
    feature_names = tfidf.get_feature_names() 
    dense = tfidf_matrix.todense()
    for i in range(len(df)):
        text = dense[i].tolist()[0]
        phrase_scores = [pair for pair in zip(range(0, len(text)), text) if pair[1] > 0]
        sorted_phrase_scores = sorted(phrase_scores, key=lambda t: t[1] * -1)
        tfidf_ranking = []
        for phrase, score in [(feature_names[word_id], score) for (word_id, score) in sorted_phrase_scores][:6]:
            tfidf_ranking.append(phrase)
        ans.append(tfidf_ranking)
    return ans

def main_key_words(topic):
    ans = {}
    for i in range(len(topic)):
        for j in range(len(topic[i])):
            word = topic[i][j]
            if word in ans:
                ans[word] += 1
            else:
                ans[word] = 1
    sorted_dictionary = sorted(ans.items() , key=lambda t : t[1] , reverse=True)
    ans = {}
    for el in sorted_dictionary[2:10]:
        ans[el[0]] = el[1]
    return ans

def trendswords(ctop, ptop):
    for key, value in ptop.items():
        if key in ctop:
            diff = ctop[key] - ptop[key]
            if diff > 0:
                ctop[key] = diff
            else:
                del ctop[key]
    sorted_dictionary = sorted(ctop.items() , key=lambda t : t[1] , reverse=True)
    ans = {}
    for el in sorted_dictionary:
        ans[el[0]] = el[1]
    return ans

def trendout(df, twords):
    ans = {}
    keywords = topics(df)
    for i in range(len(keywords)):
        ball = 0
        for key, value in twords.items():
            if key in keywords[i]:
                ball += int(value)
        ans[i] = ball
    sorted_dictionary = sorted(ans.items() , key=lambda t : t[1] , reverse=True)[0:5]
    links = []
    for el in sorted_dictionary:
        links.append(df.iloc[int(el[0])].links)
    return links

def trendlinks(df):
    current_week_filter = df.date.max() - pd.Timedelta(7, "d")
    previous_week_filter = current_week_filter - pd.Timedelta(7, "d")
    currentw_data = prime_news[prime_news.date > current_week_filter]
    previousw_data = prime_news[(prime_news.date < current_week_filter) & (prime_news.date > previous_week_filter)]
    currentw_topics = topics(currentw_data)
    previousw_topics = topics(previousw_data)
    currentw_keywords = main_key_words(currentw_topics)
    previousw_keywords = main_key_words(previousw_topics)
    finallinks = trendout(currentw_data, trendswords(currentw_keywords, previousw_keywords))
    return finallinks

trendlinks2 = trendlinks(prime_news)


# In[17]:


bs = news_for_accountant.head(3)
ac = news_for_business.head(3)
bs["role"] = 'business'
ac["role"] = 'accountant'
bsac2 = pd.concat([bs, ac], axis=0, ignore_index=True)
bsac2.to_csv("concatArtur.csv", sep=",", index=False)


# In[18]:


trendlinks2 = pd.Series(trendlinks2)
trendlinks2.to_pickle("trendlinks")


# In[19]:


bsac2


# In[20]:


trendlinks2


# In[ ]:








