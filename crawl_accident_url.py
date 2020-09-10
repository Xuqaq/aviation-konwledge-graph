import requests ##导入requests
from bs4 import BeautifulSoup ##导入bs4中的BeautifulSoup
import os
import re
import pandas as pd
import numpy as np
import time
from fake_useragent import UserAgent
from py2neo import Graph, Node, Relationship, cypher


root_url = "https://aviation-safety.net/database/"
page_head_url = "https://aviation-safety.net/database/dblist.php"
accident_head_url = "https://aviation-safety.net"


headers = {'User-Agent':"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"}##浏览器请求头（大部分网站没有这个请求头会报错、请务必加上哦）

# dict = {}
# dict['Status'] = ""
# dict['Date'] = ""
# dict['Time'] = ""
# dict['Type'] = ""
# dict['Operator'] = ""
# dict['Registration'] = ""
# dict['C/n / msn'] = ""
# dict['First flight'] = ""
# dict['Engines'] = ""
# dict['Crew'] = ""
# dict['Passengers'] = ""
# dict['Total'] = ""
# dict['Aircraft damage'] = ""
# dict['Aircraft fate'] = ""
# dict['Location'] = ""
# dict['Phase'] = ""
# dict['Nature'] = ""
# dict['Departure airport'] = ""
# dict['Destination airport'] = ""
# dict['Narrative'] = ""
# dict['Sources'] = ""
# dict['Photos'] = []



def crawl_year_main_page_url():
    start_html = requests.get(root_url, headers=headers)
    Soup = BeautifulSoup(start_html.text, 'lxml')

    url_list = Soup.find_all("a", href=re.compile("dblist.*"))
    url_list = [root_url + url['href'] for url in url_list]

    return url_list

def get_year_all_content_url(year_first_page_url):
    """
    输入了year_fisrt_page_url，抽取第2，3，...的url
    :return: year_page_urls 存储了当年时间表级别的url
    """
    first_page_html = requests.get(year_first_page_url, headers=headers)
    first_page_html_text = first_page_html.text.replace("\t", "").replace("\n", "")
    first_page_soup = BeautifulSoup(first_page_html_text, 'lxml')
    #     抽取其他page
    year_page_urls = [year_first_page_url]
    page_html = first_page_soup.find('div', class_='pagenumbers')
    if page_html is not None:
        page_divs = page_html.find_all("a")
        year_page_urls = year_page_urls + [page_head_url + url['href'] for url in page_divs]

    return year_page_urls

def get_year_url_list(have_year_url):
    have_year_url = True
    # 存储年份的url
    if not have_year_url:
        year_url_list = crawl_year_main_page_url()
        with open('./url/year_urls.txt', 'w') as f:
            for url in year_url_list:
                f.write(url+"\n")
    else:
        year_url_list = []
        with open('./url/year_urls.txt','r') as f:
            while True:
                line = f.readline()
                if not line:
                    break
                line = line.strip('\n')
                year_url_list.append(line)
    return year_url_list

def get_accident_pages_url(have_accident_pages_url, *year_url_list):
    if not have_accident_pages_url:
        with open('./url/accident_tabe_page.txt', 'a') as f:
            for test_page_main_url in year_url_list:
                print(test_page_main_url)
                headers['User-Agent'] = UserAgent().random
                accident_pages_url = get_year_all_content_url(test_page_main_url)
                for url in accident_pages_url:
                    print(url)
                    f.write(url + "\n")
#     读取
    accident_pages_url = []
    with open('./url/accident_tabe_page.txt', 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip('\n')
            accident_pages_url.append(line)
    return accident_pages_url

def get_accident_urls(accident_page_url):
    '''
    解析并获得事故列表页面的事故url
    :param accident_page_url:
    :return:
    '''
    headers['User-Agent'] = UserAgent().random
    start_html = requests.get(accident_page_url, headers=headers)
    Soup = BeautifulSoup(start_html.text, 'lxml')

    url_list = []
    url_list = Soup.find_all('a', attrs={"href": re.compile(r"^/database/record.php")})
    url_list = [accident_head_url + url['href'] for url in url_list]

    return url_list

def get_accident_info(accident_url):
    headers['User-Agent'] = UserAgent().random
    start_html = requests.get(accident_url, headers=headers)
    Soup = BeautifulSoup(start_html.text, 'lxml')

    attributes = {}
    attributes_tag = Soup.find('table').find_all('tr')
    for attribute_tag in attributes_tag:
        attribute_tag = [v.text for v in attribute_tag.find_all('td')]
        key = attribute_tag[0][:-1].strip()
        value = ""
        if len(attribute_tag)>1:
            value = attribute_tag[1].strip()
        attributes[key] = value

    # class ="innertube"
    attributes['Narrative'] = Soup.find('span',lang='en-US').text.strip()

    Accident_investigation_tag= Soup.find('div',class_='infobox2')
    if Accident_investigation_tag is not None:
        attributes['Accident investigation'] = {}
        attributes_tag = Soup.find('div',class_='infobox2').find_all('tr')
        for attribute_tag in attributes_tag:
            attribute_tag = [v.text for v in attribute_tag.find_all('td')]
            key = attribute_tag[0][:-1].strip()
            value = ""
            if len(attribute_tag) > 1:
                value = attribute_tag[1].strip()
            attributes['Accident investigation'][key] = value

    if Soup.find('div', text=re.compile('Classification:')) is not None:
    #     事故分类列表
        attributes['Classification'] = []
        for calssification_tag in Soup.find_all('a', attrs={"href": re.compile(r"^/database/events/dblist.php\?Event")}):
            attributes['Classification'].append(calssification_tag.text.strip())

    if len(attributes['Destination airport'].split(','))>1:
        attributes['Destination country'] = attributes['Destination airport'].split(',')[1].strip()
        attributes['Destination airport'] = attributes['Destination airport'].split(',')[0].strip()

    if len(attributes['Departure airport'].split(','))>1:
        attributes['Departure country'] = attributes['Departure airport'].split(',')[1].strip()
        attributes['Departure airport'] = attributes['Departure airport'].split(',')[0].strip()

    # Location': 'Verona ( \xa0 Italy)
    p1 = re.compile(r'[(](.*?)[)]', re.S)  # 最小匹配

    attributes['Location'] = attributes['Location'].split('(')[0]+ ","+ re.findall(p1, attributes['Location'])[0].strip()

    return attributes
    # Soup.find('table').find_all('tr')[0].contents
    # attributes  = Soup.find_all('tr')
    # for attribute in attributes:


if __name__ == '__main__':
    # have_year_url = True
    # year_url_list = get_year_url_list(have_year_url)  # 获取year accident url  first_page

    # have_accident_pages_url = True
    # accident_pages_url = get_accident_pages_url(have_accident_pages_url, year_url_list) # 获取
    # accident_pages_url = get_accident_pages_url(have_accident_pages_url) # 获取


    #  accident level
    # with open('./url/accident_url.txt', 'a') as f:
    #     for accident_page_url in accident_pages_url:
    #         print(accident_page_url)
    #         accidents_url_list = get_accident_urls(accident_page_url)
    #         for url in accidents_url_list:
    #             f.write(url + "\n")

    # accidents_url_list = []
    with open('./url/accident_url.txt', 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip('\n')
            print(get_accident_info(line))
    #         accidents_url_list.append(line)
    #       解析事故页面
    # accident_url = "https://aviation-safety.net/database/record.php?id=20200203-0"
    # accident_url = "https://aviation-safety.net/database/record.php?id=20200807-0"
    # accident_info = get_accident_info(accident_url)
    # print(accident_info)
    # 存储到json中


