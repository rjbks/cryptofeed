import asyncio

import aioredis

from cryptofeed.order_books.redis.utils import load_redis_script
from cryptofeed.order_books.redis.config import LUA_SCRIPT_PATHS, REDIS_URI


class RedisPool(object):
    """
    Redis pool singleton
     Lua scripts are used for any behavior which
     needs to wrap reads and writes in an atomic transaction
    """
    _pool = None

    def __new__(cls):
        if not RedisPool._pool:
            loop = asyncio.get_event_loop()
            pool = loop.run_until_complete(
                # TODO: load redis uri from config file
                aioredis.create_redis_pool(REDIS_URI, encoding='utf-8')
            )
            # TODO: load paths from config file

            for path in LUA_SCRIPT_PATHS:
                load_redis_script(path, pool=pool, loop=loop)

            RedisPool._pool = pool
        return RedisPool._pool
