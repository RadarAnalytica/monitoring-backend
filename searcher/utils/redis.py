from settings import REDIS_HOST, REDIS_PORT

from redis.asyncio import Redis

searcher_redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
