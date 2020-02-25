import time
from helpers import get_page
import asyncio
import aiohttp
import motor.motor_asyncio
import multiprocessing
from pyquery import PyQuery as pq
import requests
import re
import json
from pprint import pprint
import logging
logging.basicConfig(filename = 'ce_meta1_log.txt', level = logging.DEBUG, filemode = 'w')

async def issue_insert(ele, meta_coll):
    if(len(ele)==0):
        return
    logging.info(f'DB OP: Inserting element w/ ID: {ele[0]["id"]}')
    await meta_coll.find_one_and_update({"id":ele[0]['id']},{"$set":ele[0]}, upsert= True)

async def ins_md(careers_json, start, width):
    tasks = []
    client = motor.motor_asyncio.AsyncIOMotorClient()
    working_db = client['careersexplorer']
    meta_coll = working_db['meta1']
    #pprint(careers_json)
    logging.info(f'Creating {width} async tasks')
    for i in range(width):
        task = asyncio.Task(issue_insert(careers_json[start+i:start+i+1], meta_coll))
        tasks.append(task)
    await asyncio.gather(*tasks)

def ce_driver(careers_json, start, width):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ins_md(careers_json, start, width))

def ce_get_meta_data1(no_processes):
    ce_url = 'https://www.careerexplorer.com/careers/?count=90&direction=asc&page=1&sort=name'
    logging.info(f'Hitting API url:{ce_url}')
    resp_obj = requests.get(ce_url)
    if(resp_obj is None):
        logging.error(f'Response object is None')
    pq_obj = pq(resp_obj.text)
    script_tag = pq_obj('body').children('script')[8]
    info_master = pq(script_tag).text()
    careers_str = re.findall("window.CAREERS_JSON = .* window.CAREER_COMPATIBILITIES_JSON =", info_master)[0][21:-38]
    careers_json = json.loads(careers_str)
    per_process = len(careers_json)//no_processes + 1
    #pprint(careers_json[0])
    with multiprocessing.Pool(no_processes) as p:
        logging.info(f'Initializing {no_processes} worker processes')
        multi = [p.apply_async(ce_driver,(careers_json,i*per_process, per_process, )) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()

if __name__=='__main__':
    PROCESSES = 8
    start = time.time()
    ce_get_meta_data1(PROCESSES)
    end = time.time()
    print(str(end-start)+' seconds')