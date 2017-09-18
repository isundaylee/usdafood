import redis
import json

class Scheduler:
    def __init__(self, redis_host, redis_port = 6379, redis_db = 0):
        self.r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

    def enqueue(self, food_id):
        self.r.zadd('food_ids', food_id, str(food_id))
        self.r.zadd('queue', food_id, str(food_id))

    def dequeue(self):
        # We don't have to lock here
        # Worst case, we hand out the same food_id more than once
        # In this case we'll still have complete and correct result
        food_ids = self.r.zrange('queue', 0, 0)
        if len(food_ids) == 0:
            return None
        food_id = food_ids[0]
        self.r.zrem('queue', food_id)

        return food_id

    def submit(self, food_id, food_data):
        print("Submitting food %s" % food_id)
        self.r.set('detail_' + food_id, json.dumps(food_data))
        self.r.zadd('done', food_id, str(food_id))
