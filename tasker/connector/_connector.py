from .. import logger


class Connector:
    name = 'Connector'

    def __init__(
        self,
    ):
        self.logger = logger.logger.Logger(
            logger_name='Connector',
        )

    def key_set(
        self,
        key,
        value,
        ttl=None,
    ):
        raise NotImplementedError()

    def key_get(
        self,
        key,
    ):
        raise NotImplementedError()

    def key_delete(
        self,
        key,
    ):
        raise NotImplementedError()

    def queue_pop(
        self,
        queue_name,
    ):
        raise NotImplementedError()

    def queue_pop_bulk(
        self,
        queue_name,
        number_of_items,
    ):
        raise NotImplementedError()

    def queue_push(
        self,
        queue_name,
        item,
        priority,
    ):
        raise NotImplementedError()

    def queue_push_bulk(
        self,
        queue_name,
        items,
        priority,
    ):
        raise NotImplementedError()

    def queue_length(
        self,
        queue_name,
    ):
        raise NotImplementedError()

    def queue_delete(
        self,
        queue_name,
    ):
        raise NotImplementedError()

    def set_add(
        self,
        set_name,
        value,
    ):
        raise NotImplementedError()

    def set_remove(
        self,
        set_name,
        value,
    ):
        raise NotImplementedError()

    def set_contains(
        self,
        set_name,
        value,
    ):
        raise NotImplementedError()

    def __getstate__(
        self,
    ):
        raise NotImplementedError()

    def __setstate__(
        self,
        value,
    ):
        raise NotImplementedError()
