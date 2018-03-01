import pymongo
import datetime

from . import _connector


class Connector(
    _connector.Connector,
):
    name = 'mongo'

    never_expiration_date = datetime.datetime.utcnow() + datetime.timedelta(
        days=10000,
    )

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
        self.connection.tasker.results.create_index(
            keys='expiration_date',
            expireAfterSeconds=0,
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
        ttl=None,
    ):
        if ttl is None:
            expiration_date = self.never_expiration_date
        else:
            expiration_date = datetime.datetime.utcnow() + datetime.timedelta(
                seconds=ttl / 1000,
            )

        update_one_result = self.connection.tasker.results.update_one(
            filter={
                'key': key,
                'expiration_date': {
                    '$gt': datetime.datetime.utcnow(),
                }
            },
            update={
                '$setOnInsert': {
                    'key': key,
                    'expiration_date': expiration_date,
                },
                '$set': {
                    'value': value,
                },
            },
            upsert=True,
        )

        if update_one_result.upserted_id is not None:
            return True

        if update_one_result.modified_count == 0:
            return False
        else:
            return True

    def key_get(
        self,
        key,
    ):
        document = self.connection.tasker.results.find_one(
            filter={
                'key': key,
                'expiration_date': {
                    '$gt': datetime.datetime.utcnow(),
                },
            },
        )

        if document:
            return document['value']
        else:
            return None

    def key_del(
        self,
        keys,
    ):
        self.connection.tasker.results.delete_many(
            filter={
                'key': {
                    '$in': keys,
                },
            },
        )

    def pop(
        self,
        key,
        timeout=0,
    ):
        document = self.connection.tasker.task_queue.find_one_and_delete(
            filter={
                'queue_name': key,
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

    def pop_bulk(
        self,
        key,
        count,
    ):
        documents = []

        for i in range(count):
            document = self.connection.tasker.task_queue.find_one_and_delete(
                filter={
                    'queue_name': key,
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

    def push(
        self,
        key,
        value,
        priority,
    ):
        if priority == 'HIGH':
            priority_value = 0
        elif priority == 'NORMAL':
            priority_value = 1

        insert_one_result = self.connection.tasker.task_queue.insert_one(
            document={
                'queue_name': key,
                'priority': priority_value,
                'value': value,
            }
        )

        return insert_one_result.acknowledged

    def push_bulk(
        self,
        key,
        values,
        priority,
    ):
        if priority == 'HIGH':
            priority_value = 0
        elif priority == 'NORMAL':
            priority_value = 1

        insert_many_result = self.connection.tasker.task_queue.insert_many(
            documents=[
                {
                    'queue_name': key,
                    'priority': priority_value,
                    'value': value,
                }
                for value in values
            ]
        )

        return insert_many_result.acknowledged

    def add_to_set(
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

    def remove_from_set(
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

    def is_member_of_set(
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

    def len(
        self,
        key,
    ):
        queue_length = self.connection.tasker.task_queue.count(
            filter={
                'queue_name': key,
            },
        )

        return queue_length

    def delete(
        self,
        key,
    ):
        result = self.connection.tasker.task_queue.delete_many(
            filter={
                'queue_name': key,
            },
        )

        return result.deleted_count

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
