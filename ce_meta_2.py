import multiprocessing
from pyquery import PyQuery as pq
import requests
import re
import json
from pprint import pprint
from pymongo import MongoClient
from helpers import get_page
import time
import logging
import motor.motor_asyncio
import aiohttp
import asyncio
logging.basicConfig(filename='get_meta2_log.txt', level = logging.DEBUG, filemode = 'w')

async def issue_insertd2(conn, ele):
    if(not ele):
        return
    conn.find_one_and_update({"meta1.id":ele['meta1']['id']},{'$set':ele}, upsert=True)

async def hit_page_and_store(search_queue):
    #print(search_queue.qsize())
    if(search_queue.empty()):
        return
    slug_item = await search_queue.get()
    slug = slug_item['slug']
    hit_url = f'https://www.careerexplorer.com/careers/{slug}/salary/'
    #print(hit_url)
    page_html = await get_page(hit_url)
    pq_obj = pq(page_html)
    sal_obj = pq_obj('#salary-percentiles-container').attr('data-percentiles')
    sal_json_obj = json.loads(sal_obj)
    sal_dict_list = []
    for i in sal_json_obj:
        sal_dict = {}
        sal_dict['percentile'] = i['percentile']
        sal_dict['salary'] = i['yearly']
        sal_dict_list.append(sal_dict)
    master_data_dict = {}
    master_data_dict['meta1'] = slug_item
    #print(slug_item['id'])
    master_data_dict['data'] = sal_dict_list
    client = motor.motor_asyncio.AsyncIOMotorClient()
    working_db = client['careersexplorer']
    data_coll = working_db['salary_noloc']
    #pprint(master_data_dict)
    #print(master_data_dict['meta1']['id'])
    await issue_insertd2(data_coll, master_data_dict)
    loc_obj_list = pq_obj("#salary-by-state").children("table.Box").children("tbody").children("tr")
    loc_list = []
    for i in loc_obj_list:
        loc_dict = {}
        row_list = pq(i).children()
        loc_dict[pq(row_list[0]).text()] = pq(row_list[0]).children("a").attr("href")
        loc_list.append(loc_dict)
    meta2_dict = {'Locations':loc_list, 'meta1':slug_item}
    meta2_coll = working_db['meta2']
    await issue_insertd2(meta2_coll, meta2_dict)

async def ins_md2(meta1_queue, process_queue_size):
    search_queue = asyncio.Queue()
    for i in range(process_queue_size):
        if(not meta1_queue.empty()):
            await search_queue.put(meta1_queue.get())
    print(search_queue.qsize())
    logging.info(f'Initiated async queues of {process_queue_size}')
    logging.info(f'Worker async queue size:{search_queue.qsize()}')
    #print(search_queue.qsize())
    tasks = []
    div_factor = 2
    times = search_queue.qsize() // div_factor
    for i in range(search_queue.qsize()):
        task = asyncio.Task(hit_page_and_store(search_queue))
        tasks.append(task)
    await asyncio.gather(*tasks)
    """for _ in range(times + 1):
        await asyncio.sleep(0.2)
        
        logging.info(f'Initiating {times} batch tasks')
        await asyncio.gather(*tasks)"""

def ce_driverm2(process_queue_size, meta1_queue):
    ce2loop = asyncio.get_event_loop()
    ce2loop.run_until_complete(ins_md2(meta1_queue, process_queue_size))

def ce_get_meta_data2(no_processes):
    meta1_queue = get_meta1_q()
    logging.info(f'Got Queue of size:{(meta1_queue.qsize())}')
    process_queue_size = (meta1_queue.qsize() // no_processes) + 1
    print(meta1_queue.qsize())
    with multiprocessing.Pool(no_processes) as p:
        logging.info(f'Initiating {no_processes} pool workers')
        multi = [p.apply_async(ce_driverm2, (process_queue_size, meta1_queue, )) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()

def get_meta1_q():
    client = MongoClient()
    harvests_db = client['careersexplorer']
    meta1_coll = harvests_db['meta1']
    meta1_queue = multiprocessing.Manager().Queue()
    res = meta1_coll.find({})
    for i in list(res):
        meta1_queue.put(i)
    return meta1_queue

if __name__=='__main__':
    PROCESSES = 8
    start = time.time()
    ce_get_meta_data2(PROCESSES)
    end = time.time()
    print(str(end-start)+' seconds')