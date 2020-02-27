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
logging.basicConfig(filename='get_data3_log.txt', level = logging.DEBUG, filemode = 'w')
#Problem : Some location data not available
async def issue_insertd3(conn, ele):
    if(not ele):
        return
    conn.find_one_and_update({"id_custom":ele['id_custom']},{'$set':ele}, upsert=True)

async def hit_d3_and_store(search_queue):
    if(search_queue.empty()):
        return
    slug_item = await search_queue.get()
    #print(slug_item)
    slug_list = slug_item['Locations']
    base_url = f'https://www.careerexplorer.com'
    if(not slug_list):
        loc_sals = []
    else:
        for i in slug_list:
            url_final = base_url + list(i.values())[0]
            print(url_final)
            page_html = await get_page(url_final)
            pq_obj = pq(page_html)
            sal_obj = pq_obj('#salary-percentiles-container').attr('data-percentiles')
            sal_json_obj = json.loads(sal_obj)
            print(sal_json_obj)
            sal_dict_list = []
            for j in sal_json_obj:
                sal_dict = {}
                sal_dict['percentile'] = j['percentile']
                sal_dict['salary'] = j['yearly']
                sal_dict_list.append(sal_dict)
            master_data_dict = {}
            master_data_dict['meta2'] = slug_item
            #print(slug_item['id'])
            master_data_dict['data'] = sal_dict_list
            master_data_dict['Location'] = list(i.keys())[0]
            master_data_dict['id_custom'] = list(i.values())[0]
            master_data_dict['Job Title'] = slug_item['meta1']['name']
            client = motor.motor_asyncio.AsyncIOMotorClient()
            working_db = client['careersexplorer']
            data_coll = working_db['salary_loc']
            #pprint(master_data_dict)
            #print(master_data_dict['meta1']['id'])
            await issue_insertd3(data_coll, master_data_dict)

async def ins_d3(meta2_queue, process_queue_size):
    search_queue = asyncio.Queue()
    for i in range(process_queue_size):
        if (not meta2_queue.empty()):
            await search_queue.put(meta2_queue.get())
    print(search_queue.qsize())
    logging.info(f'Initiated async queues of {process_queue_size}')
    logging.info(f'Worker async queue size:{search_queue.qsize()}')
    # print(search_queue.qsize())
    tasks = []
    div_factor = 10
    times = search_queue.qsize() // div_factor
    for _ in range(times + 1):
        await asyncio.sleep(0.2)
        logging.info(f'Initiating {times} batch tasks')
        for i in range(search_queue.qsize()):
            task = asyncio.Task(hit_d3_and_store(search_queue))
            tasks.append(task)
        await asyncio.gather(*tasks)

def ce_driverd3(process_queue_size, meta2_queue):
    ce2loop = asyncio.get_event_loop()
    ce2loop.run_until_complete(ins_d3(meta2_queue, process_queue_size))

def ce_get_data3(no_processes):
    meta2_queue = get_meta2_q()
    logging.info(f'Got Queue of size:{(meta2_queue.qsize())}')
    process_queue_size = (meta2_queue.qsize() // no_processes) + 1
    print(meta2_queue.qsize())
    with multiprocessing.Pool(no_processes) as p:
        logging.info(f'Initiating {no_processes} pool workers')
        multi = [p.apply_async(ce_driverd3, (process_queue_size, meta2_queue, )) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()

def get_meta2_q():
    client = MongoClient()
    harvests_db = client['careersexplorer']
    meta1_coll = harvests_db['meta2']
    meta1_queue = multiprocessing.Manager().Queue()
    res = meta1_coll.find({})
    for i in list(res):
        meta1_queue.put(i)
    return meta1_queue

if __name__=='__main__':
    PROCESSES = 8
    start = time.time()
    ce_get_data3(PROCESSES)
    end = time.time()
    print(str(end-start)+' seconds')