import pymongo
import datetime

from . import _connector


class Connector(
    _connector.Connector,
):
    name = 'mongo'

    def __init__(
        self,
        mongodb_uri,
    ):
        super().__init__()

        self.mongodb_uri = mongodb_uri

        self.connection = pymongo.MongoClient(
            host=mongodb_uri,
        )

        self.connection.tasker.task_queue.create_index(
            keys=[
                (
                    'queue_name',
                    pymongo.DESCENDING,
                ),
                (
                    'priority',
                    pymongo.ASCENDING,
                ),
            ],
            background=True,
        )
        self.connection.tasker.results.create_index(
            keys=[
                (
                    'value',
                    pymongo.ASCENDING,
                ),
            ],
            background=True,
        )
        self.connection.tasker.sets.create_index(
            keys=[
                (
                    'set_name',
                    pymongo.ASCENDING,
                ),
                (
                    'value',
                    pymongo.ASCENDING,
                ),
            ],
            background=True,
            unique=True,
        )

    def key_set(
        self,
        key,
        value,
    ):
        update_one_result = self.connection.tasker.results.update_one(
            filter={
                'key': key,
            },
            update={
                '$set': {
                    'key': key,
                    'value': value,
                },
            },
            upsert=True,
        )

        if update_one_result.upserted_id is not None:
            return True
        else:
            return False

    def key_get(
        self,
        key,
    ):
        document = self.connection.tasker.results.find_one(
            filter={
                'key': key,
            },
        )

        if document:
            return document['value']
        else:
            return None

    def key_delete(
        self,
        key,
    ):
        delete_one_result = self.connection.tasker.results.delete_one(
            filter={
                'key': key,
            },
        )

        return delete_one_result.deleted_count > 0

    def queue_pop(
        self,
        queue_name,
    ):
        document = self.connection.tasker.task_queue.find_one_and_delete(
            filter={
                'queue_name': queue_name,
            },
            projection={
                'value': 1,
            },
            sort=[
                (
                    'priority',
                    pymongo.ASCENDING,
                ),
            ],
        )

        if document:
            return document['value']
        else:
            return None

    def queue_pop_bulk(
        self,
        queue_name,
        number_of_items,
    ):
        documents = []

        for i in range(number_of_items):
            document = self.connection.tasker.task_queue.find_one_and_delete(
                filter={
                    'queue_name': queue_name,
                },
                projection={
                    'value': 1,
                },
                sort=[
                    (
                        'priority',
                        pymongo.ASCENDING,
                    ),
                ],
            )

            if document:
                documents.append(document['value'])
            else:
                break

        return documents

    def queue_push(
        self,
        queue_name,
        item,
        priority='NORMAL',
    ):
        if priority == 'HIGH':
            priority_value = 0
        elif priority == 'NORMAL':
            priority_value = 1

        insert_one_result = self.connection.tasker.task_queue.insert_one(
            document={
                'queue_name': queue_name,
                'priority': priority_value,
                'value': item,
            }
        )

        return insert_one_result.acknowledged

    def queue_push_bulk(
        self,
        queue_name,
        items,
        priority='NORMAL',
    ):
        if priority == 'HIGH':
            priority_value = 0
        elif priority == 'NORMAL':
            priority_value = 1

        insert_many_result = self.connection.tasker.task_queue.insert_many(
            documents=[
                {
                    'queue_name': queue_name,
                    'priority': priority_value,
                    'value': item,
                }
                for item in items
            ]
        )

        return insert_many_result.acknowledged

    def queue_length(
        self,
        queue_name,
    ):
        queue_length = self.connection.tasker.task_queue.count(
            filter={
                'queue_name': queue_name,
            },
        )

        return queue_length

    def queue_delete(
        self,
        queue_name,
    ):
        result = self.connection.tasker.task_queue.delete_many(
            filter={
                'queue_name': queue_name,
            },
        )

        return result.deleted_count

    def set_add(
        self,
        set_name,
        value,
    ):
        try:
            self.connection.tasker.sets.insert_one(
                document={
                    'set_name': set_name,
                    'value': value,
                }
            )

            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    def set_remove(
        self,
        set_name,
        value,
    ):
        delete_one_result = self.connection.tasker.sets.delete_one(
            filter={
                'set_name': set_name,
                'value': value,
            }
        )

        return delete_one_result.deleted_count == 1

    def set_contains(
        self,
        set_name,
        value,
    ):
        find_one_result = self.connection.tasker.sets.find_one(
            filter={
                'set_name': set_name,
                'value': value,
            }
        )

        if find_one_result:
            return True
        else:
            return False

    def set_flush(
        self,
        set_name,
    ):
        delete_many_result = self.connection.tasker.sets.delete_many(
            filter={
                'set_name': set_name,
            }
        )

        return delete_many_result.deleted_count > 0

    def __getstate__(
        self,
    ):
        state = {
            'mongodb_uri': self.mongodb_uri,
        }

        return state

    def __setstate__(
        self,
        value,
    ):
        self.__init__(
            mongodb_uri=value['mongodb_uri'],
        )
