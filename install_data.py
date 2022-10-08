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

# HERE YOUR CODE



