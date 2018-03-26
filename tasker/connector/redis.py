import redis

from . import _connector


class Connector(
    _connector.Connector,
):
    name = 'redis'

    def __init__(
        self,
        host,
        port,
        password,
        database,
    ):
        super().__init__()

        self.host = host
        self.port = port
        self.password = password
        self.database = database

        self.connection = redis.StrictRedis(
            host=self.host,
            port=self.port,
            password=self.password,
            db=self.database,
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_connect_timeout=10,
            socket_timeout=60,
        )

    def key_set(
        self,
        key,
        value,
    ):
        is_new = self.connection.set(
            name=key,
            value=value,
            nx=True,
        )

        return is_new is True

    def key_get(
        self,
        key,
    ):
        return self.connection.get(
            name=key,
        )

    def key_delete(
        self,
        key,
    ):
        return self.connection.delete(key)

    def queue_pop(
        self,
        queue_name,
    ):
        value = self.connection.lpop(
            name=queue_name,
        )

        if value is None:
            return None
        else:
            return value

    def queue_pop_bulk(
        self,
        queue_name,
        number_of_items,
    ):
        pipeline = self.connection.pipeline()

        pipeline.lrange(queue_name, 0, number_of_items - 1)
        pipeline.ltrim(queue_name, number_of_items, -1)

        value = pipeline.execute()

        if len(value) == 1:
            return []
        else:
            return value[0]

    def queue_push(
        self,
        queue_name,
        item,
        priority='NORMAL',
    ):
        if priority == 'HIGH':
            return self.connection.lpush(queue_name, item)

        return self.connection.rpush(queue_name, item)

    def queue_push_bulk(
        self,
        queue_name,
        items,
        priority='NORMAL',
    ):
        if priority == 'HIGH':
            return self.connection.lpush(queue_name, *items)

        return self.connection.rpush(queue_name, *items)

    def queue_length(
        self,
        queue_name,
    ):
        return self.connection.llen(
            name=queue_name,
        )

    def queue_delete(
        self,
        queue_name,
    ):
        return self.connection.delete(queue_name)

    def set_add(
        self,
        set_name,
        value,
    ):
        added = self.connection.sadd(set_name, value)

        return bool(added)

    def set_remove(
        self,
        set_name,
        value,
    ):
        removed = self.connection.srem(set_name, value)

        return bool(removed)

    def set_contains(
        self,
        set_name,
        value,
    ):
        is_memeber = self.connection.sismember(
            name=set_name,
            value=value,
        )

        return is_memeber

    def set_flush(
        self,
        set_name,
    ):
        return self.connection.delete(set_name)

    def __getstate__(
        self,
    ):
        state = {
            'host': self.host,
            'port': self.port,
            'password': self.password,
            'database': self.database,
        }

        return state

    def __setstate__(
        self,
        value,
    ):
        self.__init__(
            host=value['host'],
            port=value['port'],
            password=value['password'],
            database=value['database'],
        )
