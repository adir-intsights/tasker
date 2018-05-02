import time
import argparse
import concurrent.futures
import grpc
import os

from . import backends
from . import tasker_pb2
from . import tasker_pb2_grpc


class TaskerServerServicer(
    tasker_pb2_grpc.TaskerServerServicer,
):
    def __init__(
        self,
        database_path,
    ):
        self.database_path = database_path

        os.makedirs(
            name=database_path,
            exist_ok=True,
        )
        self.databases = {}

        self.keys_database = backends.rocksdb.RocksDBKeys(
            database_path=database_path,
            database_name='all',
        )

    def get_queue_database(
        self,
        database_name,
    ):
        database_id = '{database_path}_queues_{database_name}'.format(
            database_path=self.database_path,
            database_name=database_name,
        )

        if database_id in self.databases:
            return self.databases[database_id]
        else:
            database = backends.rocksdb.RocksDBQueue(
                database_path=self.database_path,
                database_name=database_name,
            )
            self.databases[database_id] = database

            return database

    def queue_pop(
        self,
        request,
        context,
    ):
        database = self.get_queue_database(
            database_name=request.queue_name,
        )

        items = database.queue_pop(
            number_of_items=request.number_of_items,
        )

        return tasker_pb2.QueuePopResponse(
            items=items,
        )

    def queue_push(
        self,
        request,
        context,
    ):
        database = self.get_queue_database(
            database_name=request.queue_name,
        )

        item_pushed = database.queue_push(
            items=request.items,
            priority=request.priority,
        )

        return tasker_pb2.QueuePushResponse(
            success=item_pushed,
        )

    def queue_delete(
        self,
        request,
        context,
    ):
        database = self.get_queue_database(
            database_name=request.queue_name,
        )

        queue_deleted = database.queue_delete()

        return tasker_pb2.QueueDeleteResponse(
            success=queue_deleted,
        )

    def queue_length(
        self,
        request,
        context,
    ):
        database = self.get_queue_database(
            database_name=request.queue_name,
        )

        queue_length = database.queue_length()

        return tasker_pb2.QueueLengthResponse(
            queue_length=queue_length,
        )

    def key_get(
        self,
        request,
        context,
    ):
        value = self.keys_database.key_get(
            key=request.key,
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
        is_new_key = self.keys_database.key_set(
            key=request.key,
            value=request.value,
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
        key_deleted = self.keys_database.key_delete(
            key=request.key,
        )

        return tasker_pb2.KeyDeleteResponse(
            success=key_deleted,
        )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--database-path',
        help='path where the server should store the database files',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--port',
        help='on what port should the server listen',
        type=int,
        required=True,
    )

    args = parser.parse_args()

    tasker_server_servicer = TaskerServerServicer(
        database_path=args.database_path,
    )
    server = grpc.server(
        thread_pool=concurrent.futures.ThreadPoolExecutor(
            max_workers=1,
        ),
    )
    tasker_pb2_grpc.add_TaskerServerServicer_to_server(
        servicer=tasker_server_servicer,
        server=server,
    )
    server.add_insecure_port(
        address='[::]:{port}'.format(
            port=args.port,
        ),
    )
    server.start()

    while True:
        try:
            time.sleep(60 * 60 * 24)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    main()
