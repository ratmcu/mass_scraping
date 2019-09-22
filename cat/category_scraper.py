import multiprocessing
import os
import time
# split a list into evenly sized chunks
#!pip install wget
import wget
import logging
import pickle
import ast
import pandas as pd
import numpy as np
import urllib
from bs4 import BeautifulSoup
#!rm 'Living_people.pkl'
#wget.download('https://github.com/ratmcu/wiki_ner/blob/master/Living_people.pkl?raw=true')


logdir = 'data_frames'
if not os.path.exists(logdir): os.makedirs(logdir)


def add_wiki_prefix(func):
    def wrapper(url):
        url = 'http://en.wikipedia.org' + url
        return func(url)
    return wrapper

@add_wiki_prefix
def get_category_list(url):
    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page, 'html.parser')
    catlink_box = soup.find('div', attrs={'class': 'mw-normal-catlinks'}) # or infobox biography vcard
    return [ li.text for li in catlink_box.find_all('li')]

def create_category_table(url_list):
    category_df =  pd.DataFrame()
    for i, link in enumerate(url_list):
        print(i, link)
    #     cats = [category for category in get_category_list(str(link))]
        cats = get_category_list(str(link))
        df_cats =  pd.DataFrame(np.ones((1,len(cats)), dtype=np.int64), columns=cats)
        category_df = category_df.append(df_cats, sort=True)
    return category_df
    
with open('Living_people.pkl', 'rb') as f:
    js = pickle.load(f)
# js = json.load(js)    
links_js = ast.literal_eval(js)

def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]

def do_job_i(job_id, data_slice, queue):
    queue.put((job_id, create_category_table(data_slice)))
    
def do_job(in_queue, queue):
    while True:
            #try:
            #    index, url_list = saver_queue.get(block=False)
            #    queue.put((index, create_category_table(url_list)))   
            #except:
            #    pass
            #if not queue.empty():
            index, url_list = in_queue.get()
            queue.put((index, create_category_table(url_list)))   
            print('loaded a df {0}'.format(index))
            if in_queue.empty(): return
        
def save_chunk_to_disk(saver_queue, event):
    while True:
            try:
                index, df = saver_queue.get(block=True, timeout=1) 
                df.to_csv(logdir + '/' + 'category_data_frame_{0}.csv'.format(index))    
                print('saved file: category_data_frame_{0}.csv'.format(index))
            except:
                pass
            if event.is_set(): break
    print('done saving files the disk')
    
def save_chunk_to_diskss(chunk):
    index, df =  chunk
    df.to_csv(logdir + '/' + 'category_data_frame_{0}.csv'.format(index))    
    print('saved file: category_data_frame_{0}.csv'.format(index))
         
def dispatch_jobs(data, worker_number):
    category_data_frame =  pd.DataFrame()
    total = len(data)
    chunk_size = 100 #total // job_number
    #chunk_size = total // worker_number
    slice = chunks(data, chunk_size)

    saver_queue = multiprocessing.Queue(1000)
    event = multiprocessing.Event()
    disk_save_mp = multiprocessing.Process(target=save_chunk_to_disk, args=(saver_queue, event))
    #disk_save_mp.start()  
    workers = []
    in_queues = []
    
    print('queued data')    
    in_queue = multiprocessing.Queue(10000)
    for worker in range(worker_number):
        #in_queue = multiprocessing.Queue(1000)
        #in_queues.append(in_queue)
        j = multiprocessing.Process(target=do_job, args=(in_queue, saver_queue))
        workers.append(j)
    
    queue_number = 0
    
    for i, s in enumerate(slice):
#         print(i)
        in_queue.put((i, s))
        #in_queues[queue_number].put((i, s))
        #queue_number += queue_number
        #if queue_number == len(in_queues):
        #    queue_number = 0
        
    print('workers created')    
    
    for worker in workers:
        time.sleep(1)
        worker.start()
    disk_save_mp.start()    
    print('workers started')
    queue_empty = False
    running = any(p.is_alive() for p in workers)
    while running:
        #save_chunk_to_disk(saver_queue.get(block=True))
        running = any(p.is_alive() for p in workers)
    print('all in queues drained')
    while not saver_queue.empty(): pass    
    event.set()
    disk_save_mp.join()  
    return

data = links_js
start_time = time.perf_counter()
print(os.cpu_count())
cpu_count = os.cpu_count()
category_data_frame = dispatch_jobs(data, cpu_count)
end_time = time.perf_counter()      # 2
run_time = end_time - start_time 
print(run_time)
