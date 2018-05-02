import unittest
import pickle

from .. import connector


class ConnectorTestCase:
    @classmethod
    def setUpClass(
        cls,
    ):
        cls.test_queue_name = 'test_queue_name'
        cls.test_queue_item = b'test_queue_item'
        cls.test_queue_items = [
            b'test_queue_item_1',
            b'test_queue_item_2',
            b'test_queue_item_3',
            b'test_queue_item_4',
            b'test_queue_item_5',
            b'test_queue_item_6',
            b'test_queue_item_7',
            b'test_queue_item_8',
            b'test_queue_item_9',
            b'test_queue_item_10',
        ]

        cls.test_key_name = 'test_key'
        cls.test_key_value = b'test_value'

        cls.test_set_name = 'test_set'
        cls.test_set_value_one = b'test_value_1'
        cls.test_set_value_two = b'test_value_2'

    def test_key(
        self,
    ):
        for connector_object in [
            self.connector,
            self.pickled_connector,
        ]:
            connector_object.key_delete(
                key=self.test_key_name,
            )
            key_value = connector_object.key_get(
                key=self.test_key_name,
            )
            self.assertIsNone(
                obj=key_value,
            )

            key_is_new = connector_object.key_set(
                key=self.test_key_name,
                value=self.test_key_value
            )
            self.assertTrue(
                expr=key_is_new,
            )

            key_is_new = connector_object.key_set(
                key=self.test_key_name,
                value=self.test_key_value
            )
            self.assertFalse(
                expr=key_is_new,
            )

            key_value = connector_object.key_get(
                key=self.test_key_name,
            )
            self.assertEqual(
                first=key_value,
                second=self.test_key_value,
            )

            connector_object.key_delete(
                key=self.test_key_name,
            )
            key_value = connector_object.key_get(
                key=self.test_key_name,
            )
            self.assertIsNone(
                obj=key_value,
            )

            key_is_new = connector_object.key_set(
                key=self.test_key_name,
                value=self.test_key_value
            )
            self.assertTrue(
                expr=key_is_new,
            )
            connector_object.key_delete(
                key=self.test_key_name,
            )

    def test_queue(
        self,
    ):
        for connector_object in [
            self.connector,
            self.pickled_connector,
        ]:
            connector_object.queue_delete(
                queue_name=self.test_queue_name,
            )
            queue_length = connector_object.queue_length(
                queue_name=self.test_queue_name,
            )
            self.assertEqual(
                first=queue_length,
                second=0,
            )

            connector_object.queue_push(
                queue_name=self.test_queue_name,
                item=self.test_queue_item,
            )
            queue_length = connector_object.queue_length(
                queue_name=self.test_queue_name,
            )
            self.assertEqual(
                first=queue_length,
                second=1,
            )
            connector_object.queue_delete(
                queue_name=self.test_queue_name,
            )
            queue_length = connector_object.queue_length(
                queue_name=self.test_queue_name,
            )
            self.assertEqual(
                first=queue_length,
                second=0,
            )

            connector_object.queue_push(
                queue_name=self.test_queue_name,
                item=self.test_queue_item,
            )
            item = connector_object.queue_pop(
                queue_name=self.test_queue_name,
            )
            self.assertEqual(
                first=item,
                second=self.test_queue_item,
            )

            for item in self.test_queue_items:
                connector_object.queue_push(
                    queue_name=self.test_queue_name,
                    item=item,
                )

            items = []
            for i in range(len(self.test_queue_items)):
                item = connector_object.queue_pop(
                    queue_name=self.test_queue_name,
                )
                items.append(item)

            self.assertEqual(
                first=items,
                second=self.test_queue_items,
            )

            connector_object.queue_push_bulk(
                queue_name=self.test_queue_name,
                items=self.test_queue_items,
            )
            items = []
            for i in range(len(self.test_queue_items)):
                item = connector_object.queue_pop(
                    queue_name=self.test_queue_name,
                )
                items.append(item)

            self.assertEqual(
                first=items,
                second=self.test_queue_items,
            )

            connector_object.queue_push_bulk(
                queue_name=self.test_queue_name,
                items=self.test_queue_items,
            )
            items = connector_object.queue_pop_bulk(
                queue_name=self.test_queue_name,
                number_of_items=len(self.test_queue_items),
            )
            self.assertEqual(
                first=items,
                second=self.test_queue_items,
            )

            connector_object.queue_push_bulk(
                queue_name=self.test_queue_name,
                items=self.test_queue_items * 1000,
            )
            items = connector_object.queue_pop_bulk(
                queue_name=self.test_queue_name,
                number_of_items=len(self.test_queue_items) * 1000,
            )
            self.assertEqual(
                first=items,
                second=self.test_queue_items * 1000,
            )

            connector_object.queue_push_bulk(
                queue_name=self.test_queue_name,
                items=self.test_queue_items * 1000,
            )
            queue_length = connector_object.queue_length(
                queue_name=self.test_queue_name,
            )
            self.assertEqual(
                first=queue_length,
                second=len(self.test_queue_items) * 1000,
            )

            connector_object.queue_delete(
                queue_name=self.test_queue_name,
            )
            queue_length = connector_object.queue_length(
                queue_name=self.test_queue_name,
            )
            self.assertEqual(
                first=queue_length,
                second=0,
            )


class MongoConnectorTestCase(
    ConnectorTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.connector = connector.mongo.Connector(
            mongodb_uri='mongodb://localhost:27030/',
        )
        self.pickled_connector = pickle.dumps(self.connector)
        self.pickled_connector = pickle.loads(self.pickled_connector)


class RedisClusterSingleServerConnectorTestCase(
    ConnectorTestCase,
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
        self.pickled_connector = pickle.dumps(self.connector)
        self.pickled_connector = pickle.loads(self.pickled_connector)


class RedisClusterMultipleServersConnectorTestCase(
    ConnectorTestCase,
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
        self.pickled_connector = pickle.dumps(self.connector)
        self.pickled_connector = pickle.loads(self.pickled_connector)


class RedisConnectorTestCase(
    ConnectorTestCase,
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
        self.pickled_connector = pickle.dumps(self.connector)
        self.pickled_connector = pickle.loads(self.pickled_connector)


class TaskerServerConnectorTestCase(
    ConnectorTestCase,
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.connector = connector.tasker.Connector(
            host='127.0.0.1',
            port=50001,
        )
        self.pickled_connector = pickle.dumps(self.connector)
        self.pickled_connector = pickle.loads(self.pickled_connector)
