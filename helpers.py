import asyncio
import aiohttp
import logging

logging.basicConfig(filename='helper_log.txt', filemode='w', level=logging.DEBUG)

async def get_page(href='',proxy=None,redo=0,request_type='GET'):
    async with aiohttp.ClientSession() as client:
        logging.info('Hitting API Url : {0}'.format(href))
        response = await  client.request('{}'.format(request_type), href, proxy=proxy)
        logging.info('Status for {} : {}'.format(href,response.status))
        if response.status!= 200 and redo < 10:
            redo = redo + 1
            logging.warning("Response Code:" + str(response.status) +"received")
            return await get_page(href=href,proxy=None, redo=redo)
        else:
            return await response.text()