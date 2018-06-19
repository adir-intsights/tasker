import uuid
import unittest
import pickle
import datetime

from .. import connector
from .. import queue
from .. import encoder


class QueueTestCase:
    @classmethod
    def setUpClass(
        cls,
    ):
        cls.test_queue_name = 'test_queue_name'
        cls.enqueued_value_first = {
            'str': 'string1',
            'date': datetime.datetime.utcnow().timestamp(),
            'array': [
                1,
                2,
                3,
                4,
            ],
        }
        cls.enqueued_value_second = {
            'str': 'string2',
            'date': datetime.datetime.utcnow().timestamp(),
            'array': [
                2,
                3,
                4,
                5,
            ],
        }
        cls.enqueued_value_third = {
            'str': 'string3',
            'date': datetime.datetime.utcnow().timestamp(),
            'array': [
                3,
                4,
                5,
                6,
            ],
        }

        cls.test_result_id = str(uuid.uuid4())

    def test_no_compression_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def test_zlib_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='zlib',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def test_gzip_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='gzip',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def test_bzip2_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='bzip2',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def test_lzma_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='lzma',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def test_pickle_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def test_msgpack_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='msgpack',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def test_msgpack_compressed_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='zlib',
                serializer_name='msgpack',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def test_pickle_compressed_queue(
        self,
    ):
        test_queue = queue.Queue(
            connector=self.connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='zlib',
                serializer_name='pickle',
            ),
        )

        self.queue_functionality(
            test_queue=test_queue,
        )

        pickled_queue = pickle.dumps(test_queue)
        pickled_queue = pickle.loads(pickled_queue)

        self.queue_functionality(
            test_queue=pickled_queue,
        )

    def queue_functionality(
        self,
        test_queue,
    ):
        test_queue.flush(
            queue_name=self.test_queue_name,
        )
        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=0,
        )

        test_queue.enqueue(
            queue_name=self.test_queue_name,
            items=[self.enqueued_value_first],
            priority='NORMAL',
        )

        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=1,
        )

        test_queue.enqueue(
            queue_name=self.test_queue_name,
            items=[self.enqueued_value_second],
            priority='NORMAL',
        )

        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=2,
        )

        test_queue.enqueue(
            queue_name=self.test_queue_name,
            items=[self.enqueued_value_third],
            priority='NORMAL',
        )

        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=3,
        )

        returned_value_first = test_queue.dequeue(
            queue_name=self.test_queue_name,
            number_of_items=1,
        )
        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=2,
        )

        returned_value_second = test_queue.dequeue(
            queue_name=self.test_queue_name,
            number_of_items=1,
        )
        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=1,
        )

        returned_value_third = test_queue.dequeue(
            queue_name=self.test_queue_name,
            number_of_items=1,
        )
        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=0,
        )

        self.assertIn(
            member=returned_value_first[0],
            container=[
                self.enqueued_value_first,
                self.enqueued_value_second,
                self.enqueued_value_third,
            ],
        )
        self.assertIn(
            member=returned_value_second[0],
            container=[
                self.enqueued_value_first,
                self.enqueued_value_second,
                self.enqueued_value_third,
            ],
        )
        self.assertIn(
            member=returned_value_third[0],
            container=[
                self.enqueued_value_first,
                self.enqueued_value_second,
                self.enqueued_value_third,
            ],
        )

        for i in range(100):
            test_queue.enqueue(
                queue_name=self.test_queue_name,
                items=[self.enqueued_value_first],
                priority='NORMAL',
            )

        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=100,
        )

        test_queue.flush(
            queue_name=self.test_queue_name,
        )
        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=0,
        )

        test_queue.enqueue(
            queue_name=self.test_queue_name,
            items=[self.enqueued_value_first] * 1000,
            priority='NORMAL',
        )
        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=1000,
        )

        values = test_queue.dequeue(
            queue_name=self.test_queue_name,
            number_of_items=100,
        )
        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=900,
        )

        self.assertEqual(
            first=values,
            second=[self.enqueued_value_first] * 100,
        )

        values = test_queue.dequeue(
            queue_name=self.test_queue_name,
            number_of_items=900,
        )
        queue_length = test_queue.length(
            queue_name=self.test_queue_name,
        )
        self.assertEqual(
            first=queue_length,
            second=0,
        )

        self.assertEqual(
            first=values,
            second=[self.enqueued_value_first] * 900,
        )

        test_queue.remove_result(
            queue_name=self.test_queue_name,
            result_id=self.test_result_id,
        )
        result_exists = test_queue.has_result(
            queue_name=self.test_queue_name,
            result_id=self.test_result_id,
        )
        self.assertFalse(
            expr=result_exists,
        )

        result_added = test_queue.add_result(
            queue_name=self.test_queue_name,
            result_id=self.test_result_id,
        )
        self.assertTrue(
            expr=result_added,
        )
        result_added = test_queue.add_result(
            queue_name=self.test_queue_name,
            result_id=self.test_result_id,
        )
        self.assertFalse(
            expr=result_added,
        )
        result_exists = test_queue.has_result(
            queue_name=self.test_queue_name,
            result_id=self.test_result_id,
        )
        self.assertTrue(
            expr=result_exists,
        )

        result_removed = test_queue.remove_result(
            queue_name=self.test_queue_name,
            result_id=self.test_result_id,
        )
        self.assertTrue(
            expr=result_removed,
        )
        result_exists = test_queue.has_result(
            queue_name=self.test_queue_name,
            result_id=self.test_result_id,
        )
        self.assertFalse(
            expr=result_exists,
        )
        result_removed = test_queue.remove_result(
            queue_name=self.test_queue_name,
            result_id=self.test_result_id,
        )
        self.assertFalse(
            expr=result_removed,
        )


class SingleMongoQueueTestCase(
    QueueTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.connector = connector.mongo.Connector(
            mongodb_uri='mongodb://localhost:27030/',
        )


class SingleRedisQueueTestCase(
    QueueTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.connector = connector.redis.Connector(
            host='127.0.0.1',
            port=6379,
            password='e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
            database=0,
        )


class RedisClusterMultipleServersQueueTestCase(
    QueueTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.connector = connector.redis_cluster.Connector(
            nodes=[
                {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                    'database': 0,
                },
                {
                    'host': '127.0.0.1',
                    'port': 6380,
                    'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                    'database': 0,
                },
            ]
        )


class RedisClusterSingleServerQueueTestCase(
    QueueTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.connector = connector.redis_cluster.Connector(
            nodes=[
                {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                    'database': 0,
                },
            ]
        )


class TaskerServerQueueTestCase(
    QueueTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.connector = connector.tasker.Connector(
            host='127.0.0.1',
            port=50001,
        )
