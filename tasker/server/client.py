import grpc

from . import tasker_pb2
from . import tasker_pb2_grpc


class TaskerServerClient:
    def __init__(
        self,
        host,
        port,
    ):
        channel = grpc.insecure_channel(
            target='{host}:{port}'.format(
                host=host,
                port=port,
            ),
            options=None,
        )

        self.tasker_server_stub = tasker_pb2_grpc.TaskerServerStub(
            channel=channel,
        )

    def key_get(
        self,
        key,
    ):
        key_get_request = tasker_pb2.KeyGetRequest(
            key=key,
        )
        response = self.tasker_server_stub.key_get(key_get_request)

        if response.key_exists:
            return response.value
        else:
            return None

    def key_set(
        self,
        key,
        value,
    ):
        key_set_request = tasker_pb2.KeySetRequest(
            key=key,
            value=value,
        )
        response = self.tasker_server_stub.key_set(key_set_request)

        return response.key_is_new

    def key_delete(
        self,
        key,
    ):
        key_delete_request = tasker_pb2.KeyDeleteRequest(
            key=key,
        )
        response = self.tasker_server_stub.key_delete(key_delete_request)

        return response.success

    def queue_push(
        self,
        queue_name,
        items,
        priority,
    ):
        queue_push_request = tasker_pb2.QueuePushRequest(
            queue_name=queue_name,
            priority=priority,
            items=items,
        )
        response = self.tasker_server_stub.queue_push(queue_push_request)

        return response.success

    def queue_pop(
        self,
        queue_name,
        number_of_items,
    ):
        queue_pop_request = tasker_pb2.QueuePopRequest(
            queue_name=queue_name,
            number_of_items=number_of_items,
        )
        response = self.tasker_server_stub.queue_pop(queue_pop_request)

        return response.items

    def queue_delete(
        self,
        queue_name,
    ):
        queue_delete_request = tasker_pb2.QueueDeleteRequest(
            queue_name=queue_name,
        )
        response = self.tasker_server_stub.queue_delete(queue_delete_request)

        return response.success

    def queue_length(
        self,
        queue_name,
    ):
        queue_length_request = tasker_pb2.QueueLengthRequest(
            queue_name=queue_name,
        )
        response = self.tasker_server_stub.queue_length(queue_length_request)

        return response.queue_length
