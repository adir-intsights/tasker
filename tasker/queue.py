from . import logger


class Queue:
    def __init__(
        self,
        connector,
        encoder,
    ):
        self.logger = logger.logger.Logger(
            logger_name='Queue',
        )

        self.connector = connector
        self.encoder = encoder

    def dequeue(
        self,
        queue_name,
        number_of_items,
    ):
        try:
            decoded_values = []

            if number_of_items == 1:
                value = self.connector.queue_pop(
                    queue_name=queue_name,
                )
                if not value:
                    return []
                else:
                    values = [value]
            else:
                values = self.connector.queue_pop_bulk(
                    queue_name=queue_name,
                    number_of_items=number_of_items,
                )

            for value in values:
                decoded_value = self.encoder.decode(
                    data=value,
                )

                decoded_values.append(decoded_value)

            return decoded_values
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def enqueue(
        self,
        queue_name,
        items,
        priority,
    ):
        try:
            encoded_items = []

            if len(items) == 1:
                encoded_item = self.encoder.encode(
                    data=items[0],
                )

                pushed = self.connector.queue_push(
                    queue_name=queue_name,
                    item=encoded_item,
                    priority=priority,
                )

                return pushed
            else:
                for item in items:
                    encoded_item = self.encoder.encode(
                        data=item,
                    )

                    encoded_items.append(encoded_item)

                pushed = self.connector.queue_push_bulk(
                    queue_name=queue_name,
                    items=encoded_items,
                    priority=priority,
                )

                return pushed
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def length(
        self,
        queue_name,
    ):
        try:
            queue_len = self.connector.queue_length(
                queue_name=queue_name,
            )

            return queue_len
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def flush(
        self,
        queue_name,
    ):
        try:
            self.connector.queue_delete(
                queue_name=queue_name,
            )
            self.connector.set_flush(
                set_name='{queue_name}.results'.format(
                    queue_name=queue_name,
                ),
            )

            return True
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def add_result(
        self,
        queue_name,
        result_id,
    ):
        try:
            is_new_key = self.connector.key_set(
                key='{queue_name}.result.{result_id}'.format(
                    queue_name=queue_name,
                    result_id=result_id,
                ),
                value=b'1',
            )

            return is_new_key
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def remove_result(
        self,
        queue_name,
        result_id,
    ):
        try:
            key_was_removed = self.connector.key_delete(
                key='{queue_name}.result.{result_id}'.format(
                    queue_name=queue_name,
                    result_id=result_id,
                ),
            )

            return key_was_removed
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def has_result(
        self,
        queue_name,
        result_id,
    ):
        try:
            key_result = self.connector.key_get(
                key='{queue_name}.result.{result_id}'.format(
                    queue_name=queue_name,
                    result_id=result_id,
                ),
            )

            return key_result == b'1'
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def __getstate__(
        self,
    ):
        state = {
            'connector': self.connector,
            'encoder': self.encoder,
        }

        return state

    def __setstate__(
        self,
        state,
    ):
        self.__init__(
            connector=state['connector'],
            encoder=state['encoder'],
        )
