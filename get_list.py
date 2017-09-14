import sys
import sqlite3
import redis
import requests

from scraper.scheduler import Scheduler

################################################################################
# Main function logic
################################################################################

def usage():
    print('Usage: python get_list.py API_KEY')

if len(sys.argv) < 2:
    usage()
    exit()

API_KEY = sys.argv[1]
PER_PAGE = 500

################################################################################
# Scraping logic
################################################################################

def get_page(page_num):
    offset = page_num * PER_PAGE

    r = requests.get('https://api.nal.usda.gov/ndb/list', params={
        'api_key': API_KEY,
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

scheduler = Scheduler('localhost', redis_db=100)

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
