from json import JSONDecodeError

import redis
from redis import Redis, ConnectionPool
from rediscluster import RedisCluster

from meta.singleton import Singleton
from util import log_manager
from util import io_utils


class RedisConnection(metaclass=Singleton):

    def __init__(self):
        redis_config_file = "settings/redis-config.json"
        # redis_config_file = "settings/redis-cluster-config.json" # config for Redis Cluster connection
        try:
            self._instance = None
            self.log = log_manager.get_logger(self.__class__.__name__)
            self.config = io_utils.read_json(redis_config_file)
        except JSONDecodeError as json_error:
            self.log.error("Failed to load Redis config file", json_error)
        except FileNotFoundError as f_error:
            self.log.error(f"Can not locate Redis config file '{f_error.filename}'. Failed to initialize Redis Pool")
        except TypeError:
            self.log.exception("Invalid json config parsing from file")

    @property
    def instance(self):
        if not hasattr(self, '_conn') and self._instance is None:
            self.get_connection()
        return self._instance

    def get_connection(self):
        try:
            startup_nodes = self.config
            self._instance = RedisCluster(startup_nodes=startup_nodes, decode_responses=False)
            # For single Redis
            # pool = ConnectionPool(host=self.config.host, port=self.config.port,
            #                                 db=self.config.db, max_connections=self.config.poolSize)
            # self._instance = Redis(connection_pool=pool)
        except redis.ConnectionError:
            self.log.exception("Can not create connection to Redis")
        except AttributeError:
            self.log.exception("Failed to create Redis Pool due to some errors, thus can not create a Redis connection")

    def close(self):
        del self._instance
