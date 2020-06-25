import requests
from bs4 import BeautifulSoup
import datetime
from pymongo import MongoClient
import time


def get_page(link):
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
    r = requests.get(link, headers=headers)
    html = r.content
    html = html.decode('UTF-8')
    soup = BeautifulSoup(html, 'lxml')
    return soup


def get_data(post_list):
    global MongoAPI
    data_list = []
    for post in post_list:
        title_td = post.find('td', class_='p_title')
        title = title_td.find('a', id=True).text.strip()
        post_link = title_td.find('a', id=True)['href']
        post_link = 'https://bbs.hupu.com' + post_link

        author = post.find('td', class_='p_author').a.text.strip()
        author_page = post.find('td', class_='p_author').a['href']
        start_date = post.find('td', class_='p_author').contents[2]
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()

        reply_view = post.find('td', class_='p_re').text.strip()
        reply = reply_view.split('/')[0].strip()
        view = reply_view.split('/')[1].strip()
        reply_time = post.find('td', class_='p_retime').a.text.strip()
        lst_reply = post.find('td', class_='p_retime').contents[2]
        if ':' in reply_time:
            date_time = str(datetime.date.today()) + '' + reply_time
            date_time = datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M')
        else:
            date_time = datetime.datetime.strptime('2020-' + reply_time, '%Y-%m-%d').date()
            data_list.append([title, post_link, author, author_page, start_date, reply, view, lst_reply, date_time])
            return data_list
        link = 'https://bbs.hupu.com/bxj'
        soup = get_page(link)
        post_list = soup.find_all('tr', mid=True)
        data_list = get_data(post_list)
        for each in data_list:
            print(each)

        class MongoAPI(object):
            def __init__(self, db_ip, db_port, db_name, table_name):
                self.db_ip = db_ip
                self.db_port = db_port
                self.db_name = db_name
                self.table_name = table_name
                self.conn = MongoClient(host=self.db_ip, port=self.db_port)
                self.db = self.conn[self.table_name]
                self.table = self.db[self.table_name]

                def get_one(self, query):
                    return self.table.find_one(query, projection={'id': False})

                def get_all(self, query):
                    return self.table.find(query)

                def add(self, kv_dict):
                    return self.table.insert(kv_dict)

                def delete(self, query):
                    return self.table.delete_many(query)

                def check_exist(self, query):
                    ret = self.table.find_one(query)
                    return ret != None

                def update(self, query, kv_dict):
                    self.table.update_one(query, {'$set': kv_dict}, upsert=True)

    hupu_post = MongoAPI('localhost', 200, 'hupu', 'post')
    for each in data_list:
        hupu_post.add({'title':each[0],'post_link':each[1],'author':each[2],'author_page':each[3],'start_date':str(each[4]),'reply':each[5],'view':each[6],'last_reply':each[7],'last_reply_time':each[8]})
    for i in range(1,11):
        link = 'https://bbs.hupu.com/bxj-' + str(i)
        soup = get_page(link)

