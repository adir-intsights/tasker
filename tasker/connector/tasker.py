from . import _connector
from .. import server


class Connector(
    _connector.Connector,
):
    name = 'tasker'

    def __init__(
        self,
        host,
        port,
    ):
        super().__init__()

        self.connection = server.client.TaskerServerClient(
            host=host,
            port=port,
        )

        self.host = host
        self.port = port

    def key_set(
        self,
        key,
        value,
    ):
        is_new = self.connection.key_set(
            key=key,
            value=value,
        )

        return is_new

    def key_get(
        self,
        key,
    ):
        result = self.connection.key_get(
            key=key,
        )

        return result

    def key_delete(
        self,
        key,
    ):
        key_was_removed = self.connection.key_delete(
            key=key,
        )

        return key_was_removed

    def queue_pop(
        self,
        queue_name,
    ):
        items = self.connection.queue_pop(
            queue_name=queue_name,
            number_of_items=1,
        )

        if len(items) == 1:
            return items[0]
        else:
            return None

    def queue_pop_bulk(
        self,
        queue_name,
        number_of_items,
    ):
        items = self.connection.queue_pop(
            queue_name=queue_name,
            number_of_items=number_of_items,
        )

        return items

    def queue_push(
        self,
        queue_name,
        item,
        priority='NORMAL',
    ):
        self.connection.queue_push(
            queue_name=queue_name,
            items=[item],
            priority=priority,
        )

        return True

    def queue_push_bulk(
        self,
        queue_name,
        items,
        priority='NORMAL',
    ):
        self.connection.queue_push(
            queue_name=queue_name,
            items=items,
            priority=priority,
        )

        return True

    def queue_length(
        self,
        queue_name,
    ):
        return self.connection.queue_length(
            queue_name=queue_name,
        )

    def queue_delete(
        self,
        queue_name,
    ):
        self.connection.queue_delete(
            queue_name=queue_name,
        )

    def __getstate__(
        self,
    ):
        state = {
            'host': self.host,
            'port': self.port,
        }

        return state

    def __setstate__(
        self,
        value,
    ):
        self.__init__(
            host=value['host'],
            port=value['port'],
        )
