from redis import Redis
from json import loads, dumps
from utils.config import REDIS_HOST, REDIS_PORT

LIMIT_QUEUE = 10

cache = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def cache_latest_data(key: str, value: dict) -> None:
    if result := cache.get(key):
        result = loads(result)
        if len(result) >= LIMIT_QUEUE:
            result.pop(0)
        result.append(value)
    else:
        result = [value]
    cache.set(key, dumps(result))