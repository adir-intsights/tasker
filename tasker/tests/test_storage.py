import unittest
import time
import threading

from .. import connector
from .. import storage
from .. import encoder


class StorageTestCase:
    def test_lock_key(
        self,
    ):
        self.storage.release_lock_key(
            name=self.test_key,
        )
        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second={},
        )

        timer = threading.Timer(
            interval=2.0,
            function=self.storage.release_lock_key,
            args=(
                self.test_key,
            ),
        )
        timer.start()
        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
        )
        self.assertTrue(
            expr=acquired,
        )
        time_acquired = time.time()

        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second='locked',
        )

        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
        )
        self.assertTrue(
            expr=acquired,
        )
        time_released = time.time()

        lock_time = time_released - time_acquired
        self.assertTrue(
            expr=2.2 > lock_time > 1.9,
        )

        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second='locked',
        )

        self.storage.release_lock_key(
            name=self.test_key,
        )
        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second={},
        )

        self.storage.acquire_lock_key(
            name=self.test_key,
        )
        self.assertTrue(
            expr=acquired,
        )

        timer = threading.Timer(
            interval=2,
            function=self.storage.release_lock_key,
            args=(
                self.test_key,
            )
        )
        timer.start()

        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
            timeout=3,
        )
        self.assertTrue(
            expr=acquired,
        )

        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
            timeout=2,
        )
        self.assertFalse(
            expr=acquired,
        )

    def test_functions(
        self,
    ):
        self.storage.release_lock_key(
            name=self.test_key,
        )
        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second={},
        )

        acquired = self.storage.acquire_lock_key(
            name=self.test_key,
        )
        self.assertTrue(
            expr=acquired,
        )

        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second='locked',
        )

        self.storage.release_lock_key(
            name=self.test_key,
        )
        lock_key_value = self.storage.get_key(
            name=self.test_lock_key,
        )
        self.assertEqual(
            first=lock_key_value,
            second={},
        )


class SingleMongoStorageTestCase(
    StorageTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.mongo_connector = connector.mongo.Connector(
            mongodb_uri='mongodb://localhost:27030/',
        )

        self.storage = storage.storage.Storage(
            connector=self.mongo_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_key = 'test_key'
        self.test_lock_key = '_storage_{key_name}_lock'.format(
            key_name=self.test_key,
        )


class SingleRedisStorageTestCase(
    StorageTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.redis_connector = connector.redis.Connector(
            host='127.0.0.1',
            port=6379,
            password='e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
            database=0,
        )

        self.storage = storage.storage.Storage(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_key = 'test_key'
        self.test_lock_key = '_storage_{key_name}_lock'.format(
            key_name=self.test_key,
        )


class RedisClusterSingleServerStorageTestCase(
    StorageTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.redis_connector = connector.redis_cluster.Connector(
            nodes=[
                {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                    'database': 0,
                },
            ]
        )

        self.storage = storage.storage.Storage(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_key = 'test_key'
        self.test_lock_key = '_storage_{key_name}_lock'.format(
            key_name=self.test_key,
        )


class RedisClusterMultipleServersStorageTestCase(
    StorageTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.redis_connector = connector.redis_cluster.Connector(
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

        self.storage = storage.storage.Storage(
            connector=self.redis_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_key = 'test_key'
        self.test_lock_key = '_storage_{key_name}_lock'.format(
            key_name=self.test_key,
        )


class TaskerServerStorageTestCase(
    StorageTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.tasker_server_connector = connector.tasker.Connector(
            host='127.0.0.1',
            port=50001,
        )

        self.storage = storage.storage.Storage(
            connector=self.tasker_server_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_key = 'test_key'
        self.test_lock_key = '_storage_{key_name}_lock'.format(
            key_name=self.test_key,
        )
