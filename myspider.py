import sqlite3
import requests
import re
import ssl
import os
from datetime import date
import datetime
import time
from cfg import config
from bs4 import BeautifulSoup
from urllib.parse import urlparse,urljoin
import threading


def myspider():
    # Getting into html folder for saving html files
    BASE_DIR = os.path.abspath(os.getcwd())
    HTML_DIR = os.path.join(BASE_DIR, 'html')

    # Ignore SSL certificate errors
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    #Setting Connection with Database
    conn = sqlite3.connect('myspider.sqlite')
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS Links
        (id INTEGER PRIMARY KEY,
        link TEXT,
        source_link TEXT,
        is_crawled INTEGER,
        last_crawl_dt NUMERIC,
        response_status INTEGER,
        content_type TEXT,
        content_length INTEGER,
        created_at NUMERIC,
        file_path TEXT)''')

    # Check to see if we are already in progress...
    cur.execute('SELECT id FROM Links WHERE is_crawled IS NULL ORDER BY RANDOM() LIMIT 1')
    row = cur.fetchone()
    if row is not None:
        print("Restarting existing crawl.")
    else :
        starturl = config['base_url']
        #In case we want user to input some url:
        # starturl = input('Enter web url or enter: ')
        # if ( len(starturl) < 1 ) : starturl = 'http://www.github.com/harshitsurana'
        #To filter out user entered url
        if ( starturl.endswith('/') ) : starturl = starturl[:-1]
        web = starturl
        if ( starturl.endswith('.htm') or starturl.endswith('.html') ) :
            pos = starturl.rfind('/')
            web = starturl[:pos]

        if ( len(web) > 1 ) :
            cur.execute('''INSERT OR IGNORE INTO Links (link,created_at,is_crawled) VALUES ( ?, ?,?)''', ( starturl,date.today(),0 ))
            conn.commit()

    while True:
        no_of_links = cur.execute('select * from Links')
        count_of_links=len(no_of_links.fetchall())
        max_limit=int(config['max_limit'])
        if count_of_links < max_limit:
            pass
        else :
            print("Maximum limit reached")
            continue

        cur.execute('SELECT id,link FROM Links WHERE is_crawled=? ORDER BY id ASC LIMIT 1',(0,))
        try:
            row = cur.fetchone()
            url = row[1]
            id=row[0]
            # cur.execute('UPDATE Links SET is_crawled=?,last_crawl_dt=? WHERE link=?', (1,date.today(),url) )
        except:
            print('No unretrieved HTML pages found')
            continue

        try:
            #Requesting a page using request library
            r = requests.get(url)
            status_code = r.status_code
            content_type = r.headers['content-type']
            html = r.text
            content_length=len(html)
            dt= date.today()

            if int(status_code) != 200 :
                # print("Error on page: ",status_code, "url is",url)
                cur.execute('''UPDATE Links SET is_crawled =?,
                last_crawl_dt =?, response_status=?, content_type=?,
                content_length=? WHERE link= ?''',(1,dt,status_code,content_type,content_length,url) )
                conn.commit()
                continue

            #To save html using the associated id in the database
            file_name=str(id)+'.html'
            completeName = os.path.join(HTML_DIR, file_name)
            # print(completeName)
            file = open(completeName, 'w', encoding='utf-8')
            file.write(str(html))
            file.close()

            soup = BeautifulSoup(html, "html.parser")

        except KeyboardInterrupt:
            print('')
            print('Program interrupted by user...')
            break
        except:
            # print("Unable to retrieve or parse page",url)
            cur.execute('UPDATE Links SET is_crawled=?,last_crawl_dt=? WHERE link=?', (1,date.today(),url) )
            conn.commit()
            continue

        tags = soup('a')
        for tag in tags:
            href = tag.get('href', None)
            if ( href is None ) : continue
            if(href.startswith('javascript')): continue
            # Resolve relative references like href="/contact"
            up = urlparse(href)
            if ( len(up.scheme) < 1 ) :
                href = urljoin(url, href)
            ipos = href.find('#')
            qpos = href.find('?')
            if ( ipos > 1 ) : href = href[:ipos]
            if ( qpos > 1 ) : href = href[:qpos]
            if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') or href.endswith('.zip') or href.endswith('.rar')) : continue
            if ( href.endswith('/') ) : href = href[:-1]
            # print href
            if ( len(href) < 1 ) : continue
            if (href.startswith('http') or href.startswith('https') or href.startswith('www')):
                pass
            else:
                continue

            cur.execute('SELECT id FROM Links WHERE link is ? ',(href,))
            row = cur.fetchone()
            if row is not None:
                cur.execute('SELECT last_crawl_dt FROM Links WHERE link is ? ',(href,))
                row = cur.fetchone()
                date0=row[0]
                if date0 is not None:
                    date1=list(map(int,date0.split('-')))
                    date2=datetime.date(date1[0], date1[1],date1[2])
                    if date.today()>date2:
                        cur.execute('''UPDATE Links SET is_crawled =?,
                        WHERE link= ?''',(0,url) )
                        conn.commit()
                    else:
                        continue
            else:
                cur.execute('''INSERT OR IGNORE INTO Links (link, source_link,created_at,is_crawled) VALUES ( ?, ?,?,?)''', ( href,url,date.today(),0 ))
                conn.commit()

        cur.execute('''UPDATE Links SET is_crawled =?,
        last_crawl_dt =?, response_status=?, content_type=?,
        content_length=?, file_path=? WHERE link= ?''',(1,dt,status_code,content_type,content_length,completeName,url) )
        conn.commit()

        # time.sleep(int(config['sleep_time']))


# # Getting into html folder for saving html files
# BASE_DIR = os.path.abspath(os.getcwd())
# HTML_DIR = os.path.join(BASE_DIR, 'html')
#
# # Ignore SSL certificate errors
# ctx = ssl.create_default_context()
# ctx.check_hostname = False
# ctx.verify_mode = ssl.CERT_NONE
#
# #Setting Connection with Database
# conn = sqlite3.connect('myspider.sqlite')
# cur = conn.cursor()
#
# cur.execute('''CREATE TABLE IF NOT EXISTS Links
#     (id INTEGER PRIMARY KEY,
#     link TEXT,
#     source_link TEXT,
#     is_crawled INTEGER,
#     last_crawl_dt NUMERIC,
#     response_status INTEGER,
#     content_type TEXT,
#     content_length INTEGER,
#     created_at NUMERIC,
#     file_path TEXT)''')
#
# # Check to see if we are already in progress...
# cur.execute('SELECT id FROM Links WHERE is_crawled IS NULL ORDER BY RANDOM() LIMIT 1')
# row = cur.fetchone()
# if row is not None:
#     print("Restarting existing crawl.")
# else :
#     starturl = config['base_url']
#     #In case we want user to input some url:
#     # starturl = input('Enter web url or enter: ')
#     # if ( len(starturl) < 1 ) : starturl = 'http://www.github.com/harshitsurana'
#     #To filter out user entered url
#     if ( starturl.endswith('/') ) : starturl = starturl[:-1]
#     web = starturl
#     if ( starturl.endswith('.htm') or starturl.endswith('.html') ) :
#         pos = starturl.rfind('/')
#         web = starturl[:pos]
#
#     if ( len(web) > 1 ) :
#         cur.execute('''INSERT OR IGNORE INTO Links (link,created_at,is_crawled) VALUES ( ?, ?,?)''', ( starturl,date.today(),0 ))
#         conn.commit()
#
# while True:
#     no_of_links = cur.execute('select * from Links')
#     count_of_links=len(no_of_links.fetchall())
#     max_limit=int(config['max_limit'])
#     if count_of_links < max_limit:
#         pass
#     else :
#         print("Maximum limit reached")
#         continue
#
#     cur.execute('SELECT id,link FROM Links WHERE is_crawled=? ORDER BY id ASC LIMIT 1',(0,))
#     try:
#         row = cur.fetchone()
#         url = row[1]
#         id=row[0]
#         # cur.execute('UPDATE Links SET is_crawled=?,last_crawl_dt=? WHERE link=?', (1,date.today(),url) )
#     except:
#         print('No unretrieved HTML pages found')
#         continue
#
#     try:
#         #Requesting a page using request library
#         r = requests.get(url)
#         status_code = r.status_code
#         content_type = r.headers['content-type']
#         html = r.text
#         content_length=len(html)
#         dt= date.today()
#
#         if int(status_code) != 200 :
#             # print("Error on page: ",status_code, "url is",url)
#             cur.execute('''UPDATE Links SET is_crawled =?,
#             last_crawl_dt =?, response_status=?, content_type=?,
#             content_length=? WHERE link= ?''',(1,dt,status_code,content_type,content_length,url) )
#             conn.commit()
#             continue
#
#         #To save html using the associated id in the database
#         file_name=str(id)+'.html'
#         completeName = os.path.join(HTML_DIR, file_name)
#         # print(completeName)
#         file = open(completeName, 'w', encoding='utf-8')
#         file.write(str(html))
#         file.close()
#
#         soup = BeautifulSoup(html, "html.parser")
#
#     except KeyboardInterrupt:
#         print('')
#         print('Program interrupted by user...')
#         break
#     except:
#         # print("Unable to retrieve or parse page",url)
#         cur.execute('UPDATE Links SET is_crawled=?,last_crawl_dt=? WHERE link=?', (1,date.today(),url) )
#         conn.commit()
#         continue
#
#     tags = soup('a')
#     for tag in tags:
#         href = tag.get('href', None)
#         if ( href is None ) : continue
#         if(href.startswith('javascript')): continue
#         # Resolve relative references like href="/contact"
#         up = urlparse(href)
#         if ( len(up.scheme) < 1 ) :
#             href = urljoin(url, href)
#         ipos = href.find('#')
#         qpos = href.find('?')
#         if ( ipos > 1 ) : href = href[:ipos]
#         if ( qpos > 1 ) : href = href[:qpos]
#         if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') or href.endswith('.zip') or href.endswith('.rar')) : continue
#         if ( href.endswith('/') ) : href = href[:-1]
#         # print href
#         if ( len(href) < 1 ) : continue
#         if (href.startswith('http') or href.startswith('https') or href.startswith('www')):
#             pass
#         else:
#             continue
#
#         cur.execute('SELECT id FROM Links WHERE link is ? ',(href,))
#         row = cur.fetchone()
#         if row is not None:
#             cur.execute('SELECT last_crawl_dt FROM Links WHERE link is ? ',(href,))
#             row = cur.fetchone()
#             date0=row[0]
#             if date0 is not None:
#                 date1=list(map(int,date0.split('-')))
#                 date2=datetime.date(date1[0], date1[1],date1[2])
#                 if date.today()>date2:
#                     cur.execute('''UPDATE Links SET is_crawled =?,
#                     WHERE link= ?''',(0,url) )
#                     conn.commit()
#                 else:
#                     continue
#         else:
#             cur.execute('''INSERT OR IGNORE INTO Links (link, source_link,created_at,is_crawled) VALUES ( ?, ?,?,?)''', ( href,url,date.today(),0 ))
#             conn.commit()
#
#     cur.execute('''UPDATE Links SET is_crawled =?,
#     last_crawl_dt =?, response_status=?, content_type=?,
#     content_length=?, file_path=? WHERE link= ?''',(1,dt,status_code,content_type,content_length,completeName,url) )
#     conn.commit()
#
#     # time.sleep(int(config['sleep_time']))

#FOR MULTI THREADING
#COPY ABOVE CODE TO A FUNCTION
thread_list=[]
for i in range(int(config['max_thread'])):
    thread_list.append(threading.Thread(target=myspider, ))

for thread in thread_list:
    thread.start()
    time.sleep(int(config['sleep_time']))

for thread in thread_list:
    thread.join()
