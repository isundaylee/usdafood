import redis

class Scheduler:
    def __init__(self, redis_host, redis_port = 6379, redis_db = 0):
        self.r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

    def enqueue(self, food_id):
        self.r.zadd('food_ids', food_id, str(food_id))
        self.r.zadd('queue', food_id, str(food_id))

    def dequeue(self):
        raise NotImplemented

    def done(self, food_id, food_data):
        raise NotImplemented
