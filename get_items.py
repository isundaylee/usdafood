import os
import requests
import time

from scraper.utils import env
from scraper.scheduler import Scheduler

################################################################################
# Scraping logic
################################################################################

TIMEOUT = 10

def get_items(food_ids):
    retries = 5
    success = False

    while retries > 0:
        try:
            r = requests.get('https://api.nal.usda.gov/ndb/V2/reports', params={
                'api_key': env('DATA_GOV_API_KEY'),
                'format': 'json',
                'type': 'b',
                'ndbno': food_ids,
            }, timeout=TIMEOUT)

            success = True
            break
        except:
            print("Retrying...")
            retries -= 1

    if not success:
        print("Get item request failed")
        return None

    return r.json()

################################################################################
# Job logic
################################################################################

BATCH_SIZE = 50
DELAY = 0

scheduler = Scheduler(env('SCHEDULER_HOST'), redis_db=int(env('SCHEDULER_DB')))

scheduler.claim_lost()

while True:
    batch_ids = []

    while len(batch_ids) < BATCH_SIZE:
        food_id = scheduler.dequeue()
        if food_id is None:
            break
        batch_ids.append(food_id)

    if len(batch_ids) == 0:
        print("We're done!")
        break

    foods = get_items(batch_ids)
    if foods is None:
        continue

    for food in foods["foods"]:
        food_id = food["food"]["desc"]["ndbno"]
        scheduler.submit(food_id, food)

    print("Crawled %d foods." % len(batch_ids))
    time.sleep(DELAY)
