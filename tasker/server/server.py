import japronto
import japronto.app
import pickle
import rocksdb
import rocksdb.errors
import os


class TaskerServer:
    def __init__(
        self,
    ):
        os.makedirs(
            name='tasker_db',
            exist_ok=True,
        )
        self.sub_databases = {}

    def start(
        self,
    ):
        japronto_application = japronto.app.Application()
        japronto_application.router.add_route('/queue_pop', self.queue_pop, 'POST')
        japronto_application.router.add_route('/queue_push', self.queue_push, 'POST')
        japronto_application.router.add_route('/queue_delete', self.queue_delete, 'POST')
        japronto_application.router.add_route('/queue_length', self.queue_length, 'POST')
        japronto_application.router.add_route('/key_get', self.key_get, 'POST')
        japronto_application.router.add_route('/key_set', self.key_set, 'POST')
        japronto_application.router.add_route('/key_delete', self.key_delete, 'POST')
        japronto_application.router.add_route('/set_contains', self.set_contains, 'POST')
        japronto_application.router.add_route('/set_add', self.set_add, 'POST')
        japronto_application.router.add_route('/set_remove', self.set_remove, 'POST')
        japronto_application.router.add_route('/set_flush', self.set_flush, 'POST')
        japronto_application.run()

    def get_sub_database(
        self,
        database_name,
        sub_database_name,
    ):
        database_path = 'tasker_db/{database_name}/{sub_database_name}'.format(
            database_name=database_name,
            sub_database_name=sub_database_name,
        )
        if database_path in self.sub_databases:
            return self.sub_databases[database_path]

        os.makedirs(
            name=database_path,
            exist_ok=True,
        )

        sub_database = rocksdb.DB(
            db_name=database_path,
            opts=rocksdb.Options(
                create_if_missing=True,
                compression=rocksdb.CompressionType.no_compression,
            ),
        )
        self.sub_databases[database_path] = sub_database

        return sub_database

    def queue_pop(
        self,
        request,
    ):
        queue_name = request.query['queue_name']

        sub_database = self.get_sub_database(
            database_name='queues',
            sub_database_name=queue_name,
        )

        if 'number_of_items' in request.query:
            number_of_items = int(request.query['number_of_items'])
        else:
            number_of_items = 1

        items = []
        keys = []

        database_iterator = sub_database.iteritems()
        database_iterator.seek_to_first()

        items_fetched = 0
        for key, value in database_iterator:
            items.append(value)
            keys.append(key)

            items_fetched += 1
            if items_fetched == number_of_items:
                break

        database_write_batch = rocksdb.WriteBatch()
        for key in keys:
            database_write_batch.delete(key)

        sub_database.write(
            batch=database_write_batch,
            sync=True,
            disable_wal=True,
        )

        return request.Response(
            body=pickle.dumps(
                obj=items,
            ),
        )

    def queue_push(
        self,
        request,
    ):
        queue_name = request.query['queue_name']
        priority = request.query['priority']

        sub_database = self.get_sub_database(
            database_name='queues',
            sub_database_name=queue_name,
        )

        items = pickle.loads(
            data=request.body,
        )
        database_iterator = sub_database.iterkeys()

        if priority == 'NORMAL':
            database_iterator.seek_to_last()
            current_items = list(database_iterator)
            if current_items:
                last_item_key = current_items[0]
                next_item_number = int(last_item_key.decode('utf-8')) + 1
            else:
                next_item_number = int((10 ** 16) / 2) + 1
            factor = 1
        elif priority == 'HIGH':
            database_iterator.seek_to_first()
            current_items = list(database_iterator)
            if current_items:
                last_item_key = current_items[0]
                next_item_number = int(last_item_key.decode('utf-8')) - 1
            else:
                next_item_number = int((10 ** 16) / 2) - 1
            factor = -1
        else:
            raise Exception(
                'unknown priority level: {priority}'.format(
                    priority=priority,
                )
            )

        database_write_batch = rocksdb.WriteBatch()
        for item in items:
            next_item_number_bytes = str(next_item_number).rjust(20, '0').encode('utf-8')
            database_write_batch.put(
                next_item_number_bytes,
                item,
            )
            next_item_number += factor

        sub_database.write(
            batch=database_write_batch,
            sync=True,
            disable_wal=True,
        )

        return request.Response(
            code=200,
        )

    def queue_delete(
        self,
        request,
    ):
        queue_name = request.query['queue_name']

        sub_database = self.get_sub_database(
            database_name='queues',
            sub_database_name=queue_name,
        )

        database_iterator = sub_database.iterkeys()
        database_iterator.seek_to_first()

        while True:
            database_write_batch = rocksdb.WriteBatch()

            num_of_keys = 0
            num_of_keys_per_chunk = 5000

            for key in database_iterator:
                database_write_batch.delete(key)

                num_of_keys += 1
                if num_of_keys == num_of_keys_per_chunk:
                    break

            sub_database.write(
                batch=database_write_batch,
                sync=True,
                disable_wal=True,
            )

            if num_of_keys != num_of_keys_per_chunk:
                break

        return request.Response(
            code=200,
        )

    def queue_length(
        self,
        request,
    ):
        queue_name = request.query['queue_name']

        sub_database = self.get_sub_database(
            database_name='queues',
            sub_database_name=queue_name,
        )

        database_iterator = sub_database.iterkeys()
        database_iterator.seek_to_first()

        num_of_keys = 0
        for key in database_iterator:
            num_of_keys += 1

        return request.Response(
            text=str(num_of_keys),
        )

    def key_get(
        self,
        request,
    ):
        key = request.query['key']

        keys_database = self.get_sub_database(
            database_name='keys',
            sub_database_name='all',
        )

        value = keys_database.get(
            key=key.encode('utf-8'),
        )

        if value is None:
            return request.Response(
                code=404,
            )
        else:
            return request.Response(
                body=value,
            )

    def key_set(
        self,
        request,
    ):
        key = request.query['key']
        value = request.body

        keys_database = self.get_sub_database(
            database_name='keys',
            sub_database_name='all',
        )

        current_key = keys_database.get(
            key=key.encode('utf-8'),
        )
        is_new_key = current_key is None

        keys_database.put(
            key=key.encode('utf-8'),
            value=value,
            sync=True,
            disable_wal=True,
        )

        if is_new_key:
            return request.Response(
                code=200,
            )
        else:
            return request.Response(
                code=409,
            )

    def key_delete(
        self,
        request,
    ):
        key = request.query['key']

        keys_database = self.get_sub_database(
            database_name='keys',
            sub_database_name='all',
        )

        current_key = keys_database.get(
            key=key.encode('utf-8'),
        )
        key_does_not_exist = current_key is None
        if key_does_not_exist:
            return request.Response(
                code=404,
            )

        keys_database.delete(
            key=key.encode('utf-8'),
            sync=True,
            disable_wal=True,
        )

        return request.Response(
            code=200,
        )

    def set_contains(
        self,
        request,
    ):
        set_name = request.query['set_name']
        value = request.body

        sets_database = self.get_sub_database(
            database_name='sets',
            sub_database_name='all',
        )

        set_object = sets_database.get(
            key=set_name.encode('utf-8'),
        )
        if set_object is None:
            return request.Response(
                code=404,
            )

        set_object = pickle.loads(
            data=set_object,
        )
        if value in set_object:
            return request.Response(
                code=200,
            )
        else:
            return request.Response(
                code=404,
            )

    def set_add(
        self,
        request,
    ):
        set_name = request.query['set_name']
        value = request.body

        sets_database = self.get_sub_database(
            database_name='sets',
            sub_database_name='all',
        )

        set_object = sets_database.get(
            key=set_name.encode('utf-8'),
        )
        if set_object is None:
            new_set_object = set()
            new_set_object.add(value)

            sets_database.put(
                key=set_name.encode('utf-8'),
                value=pickle.dumps(
                    obj=new_set_object,
                ),
                sync=True,
                disable_wal=True,
            )

            return request.Response(
                code=200,
            )

        set_object = pickle.loads(
            data=set_object,
        )
        if value in set_object:
            return request.Response(
                code=404,
            )
        else:
            set_object.add(value)

            sets_database.put(
                key=set_name.encode('utf-8'),
                value=pickle.dumps(
                    obj=set_object,
                ),
                sync=True,
                disable_wal=True,
            )

            return request.Response(
                code=200,
            )

    def set_remove(
        self,
        request,
    ):
        set_name = request.query['set_name']
        value = request.body

        sets_database = self.get_sub_database(
            database_name='sets',
            sub_database_name='all',
        )

        set_object = sets_database.get(
            key=set_name.encode('utf-8'),
        )
        if set_object is None:
            return request.Response(
                code=404,
            )

        set_object = pickle.loads(
            data=set_object,
        )
        if value in set_object:
            set_object.remove(value)
            sets_database.put(
                key=set_name.encode('utf-8'),
                value=pickle.dumps(
                    obj=set_object,
                ),
                sync=True,
                disable_wal=True,
            )

            return request.Response(
                code=200,
            )
        else:
            return request.Response(
                code=404,
            )

    def set_flush(
        self,
        request,
    ):
        set_name = request.query['set_name']

        sets_database = self.get_sub_database(
            database_name='sets',
            sub_database_name='all',
        )

        sets_database.delete(
            key=set_name.encode('utf-8'),
            sync=True,
            disable_wal=True,
        )

        return request.Response(
            code=200,
        )


def main():
    tasker_server = TaskerServer()
    tasker_server.start()


if __name__ == '__main__':
    main()
