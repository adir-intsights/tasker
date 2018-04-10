import time
import concurrent.futures
import grpc
import rocksdb
import rocksdb.errors
import os

from . import tasker_pb2
from . import tasker_pb2_grpc


class TaskerServerServicer(
    tasker_pb2_grpc.TaskerServerServicer,
):
    def __init__(
        self,
    ):
        os.makedirs(
            name='tasker_db',
            exist_ok=True,
        )
        self.sub_databases = {}
        self.number_of_deleted_items = 0

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

        rocksdb_options = rocksdb.Options()
        rocksdb_options.create_if_missing = True
        rocksdb_options.max_open_files = 300000
        rocksdb_options.write_buffer_size = 67108864
        rocksdb_options.max_write_buffer_number = 3
        rocksdb_options.target_file_size_base = 67108864
        rocksdb_options.compression = rocksdb.CompressionType.no_compression
        rocksdb_options.table_factory = rocksdb.BlockBasedTableFactory(
            filter_policy=rocksdb.BloomFilterPolicy(
                bits_per_key=10,
            ),
            block_cache=rocksdb.LRUCache(
                capacity=2 * (1024 ** 3),
            ),
            block_cache_compressed=None,
        )

        sub_database = rocksdb.DB(
            db_name=database_path,
            opts=rocksdb_options,
        )
        self.sub_databases[database_path] = sub_database

        return sub_database

    def queue_pop(
        self,
        request,
        context,
    ):
        sub_database = self.get_sub_database(
            database_name='queues',
            sub_database_name=request.queue_name,
        )

        number_of_items = request.number_of_items

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

        if keys:
            database_write_batch = rocksdb.WriteBatch()
            for key in keys:
                database_write_batch.delete(key)

            sub_database.write(
                batch=database_write_batch,
                sync=True,
                disable_wal=True,
            )

            self.number_of_deleted_items += items_fetched
            if self.number_of_deleted_items > 10000:
                self.number_of_deleted_items = 0
                sub_database.compact_range(
                    begin=None,
                    end=keys[-1],
                )

        return tasker_pb2.QueuePopResponse(
            items=items,
        )

    def queue_push(
        self,
        request,
        context,
    ):
        sub_database = self.get_sub_database(
            database_name='queues',
            sub_database_name=request.queue_name,
        )

        items = request.items
        database_iterator = sub_database.iterkeys()

        if request.priority == 'NORMAL':
            database_iterator.seek_to_last()
            current_items = list(database_iterator)
            if current_items:
                last_item_key = current_items[0]
                next_item_number = int(last_item_key.decode('utf-8')) + 1
            else:
                next_item_number = int((10 ** 16) / 2)
            factor = 1
        elif request.priority == 'HIGH':
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
                    priority=request.priority,
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

        return tasker_pb2.QueuePushResponse(
            success=True,
        )

    def queue_delete(
        self,
        request,
        context,
    ):
        sub_database = self.get_sub_database(
            database_name='queues',
            sub_database_name=request.queue_name,
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

        sub_database.compact_range(
            begin=None,
            end=None,
        )

        return tasker_pb2.QueueDeleteResponse(
            success=True,
        )

    def queue_length(
        self,
        request,
        context,
    ):
        sub_database = self.get_sub_database(
            database_name='queues',
            sub_database_name=request.queue_name,
        )
        sub_database.compact_range(
            begin=None,
            end=None,
        )

        database_iterator = sub_database.iterkeys()

        database_iterator.seek_to_first()
        try:
            current_item = next(database_iterator)
        except StopIteration:
            return tasker_pb2.QueueLengthResponse(
                queue_length=0,
            )

        first_item_key = current_item
        first_item_number = int(first_item_key.decode('utf-8'))

        database_iterator.seek_to_last()
        current_item = next(database_iterator)
        last_item_key = current_item
        last_item_number = int(last_item_key.decode('utf-8'))

        return tasker_pb2.QueueLengthResponse(
            queue_length=last_item_number - first_item_number + 1,
        )

    def key_get(
        self,
        request,
        context,
    ):
        keys_database = self.get_sub_database(
            database_name='keys',
            sub_database_name='all',
        )

        value = keys_database.get(
            key=request.key.encode('utf-8'),
        )

        if value is None:
            return tasker_pb2.KeyGetResponse(
                key_exists=False,
                value=b'',
            )
        else:
            return tasker_pb2.KeyGetResponse(
                key_exists=True,
                value=value,
            )

    def key_set(
        self,
        request,
        context,
    ):
        keys_database = self.get_sub_database(
            database_name='keys',
            sub_database_name='all',
        )

        current_key = keys_database.get(
            key=request.key.encode('utf-8'),
        )
        is_new_key = current_key is None

        keys_database.put(
            key=request.key.encode('utf-8'),
            value=request.value,
            sync=True,
            disable_wal=True,
        )

        if is_new_key:
            return tasker_pb2.KeySetResponse(
                key_was_set=True,
                key_is_new=True,
            )
        else:
            return tasker_pb2.KeySetResponse(
                key_was_set=True,
                key_is_new=False,
            )

    def key_delete(
        self,
        request,
        context,
    ):
        keys_database = self.get_sub_database(
            database_name='keys',
            sub_database_name='all',
        )

        current_key = keys_database.get(
            key=request.key.encode('utf-8'),
        )
        key_does_not_exist = current_key is None
        if key_does_not_exist:
            return tasker_pb2.KeyDeleteResponse(
                success=False,
            )

        keys_database.delete(
            key=request.key.encode('utf-8'),
            sync=True,
            disable_wal=True,
        )

        return tasker_pb2.KeyDeleteResponse(
            success=True,
        )


def main():
    server = grpc.server(
        thread_pool=concurrent.futures.ThreadPoolExecutor(
            max_workers=1,
        ),
    )
    tasker_pb2_grpc.add_TaskerServerServicer_to_server(
        servicer=TaskerServerServicer(),
        server=server,
    )
    server.add_insecure_port(
        address='[::]:50001',
    )
    server.start()

    while True:
        try:
            time.sleep(60 * 60 * 24)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    main()
