import sys

from service import log_manager
from service.redis_connection import RedisConnection
from redis.exceptions import ConnectionError, TimeoutError, RedisError

_log = log_manager.get_logger("Redis-Operations")


def save_dict_to_redis(key, dictionary, threshold=200):
    temp_dict = {}
    success = 0
    try:
        for k, v in dictionary.items():
            temp_dict[k] = v
            if len(temp_dict) == threshold:
                RedisConnection().instance.hmset(key, temp_dict)
                success += len(temp_dict)
                temp_dict.clear()
        if temp_dict:
            RedisConnection().instance.hmset(key, temp_dict)
            success += len(temp_dict)
            temp_dict.clear()
    except ConnectionError:
        _log.exception("Redis connection error when saving dictionary")
    except TimeoutError:
        _log.exception("Redis connection timed out when trying to save dictionary")
    except RedisError:
        _log.error(f"Redis operation HSET failed when trying to "
            f"save dictionary size of {sys.getsizeof(temp_dict) >> 10} KB")
    return success
