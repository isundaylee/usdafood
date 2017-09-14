import os
import sys
import sqlite3
import redis
import requests

from scraper.utils import env
from scraper.scheduler import Scheduler

################################################################################
# Scraping logic
################################################################################

PER_PAGE = 500

def get_page(page_num):
    offset = page_num * PER_PAGE

    r = requests.get('https://api.nal.usda.gov/ndb/list', params={
        'api_key': env('DATA_GOV_API_KEY'),
        'format': 'json',
        'lt': 'f',
        'max': PER_PAGE,
        'offset': offset,
        'sort': 'id'
    })

    return map(lambda x: int(x['id']), r.json()['list']['item'])

################################################################################
# Job logic
################################################################################

scheduler = Scheduler(env('SCHEDULER_HOST'), redis_db=int(env('SCHEDULER_DB')))

current_page = 0
total_count = 0
while True:
    food_ids = get_page(current_page)
    count = 0
    for food_id in food_ids:
        count += 1
        scheduler.enqueue(food_id)
    total_count += count

    print('Crawled %d food IDs.' % total_count)

    if count < PER_PAGE:
        print("We're done! ")
        break

    current_page += 1
