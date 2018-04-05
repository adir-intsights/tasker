import msgpack

from . import _serializer


class Serializer(
    _serializer.Serializer,
):
    name = 'msgpack'

    @staticmethod
    def serialize(
        data,
    ):
        serialized_object = msgpack.dumps(data)

        return serialized_object

    @staticmethod
    def unserialize(
        data,
    ):
        unserialized_object = msgpack.loads(
            packed=data,
            raw=False,
        )

        return unserialized_object
