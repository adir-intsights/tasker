import unittest
import pickle

from .. import connector


class MongoConnectorTestCase(
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.mongo_connector = connector.mongo.Connector(
            mongodb_uri='mongodb://localhost:27030/',
        )
        self.test_key = 'test_key'
        self.set_name = 'test_set'
        self.test_value = b'test_value'
        self.test_set_value = b'test_value'

        self.mongo_connector.delete(
            key=self.test_key,
        )

    def test_connector_functionality(
        self,
    ):
        self.assertEqual(self.mongo_connector.len(self.test_key), 0)

        self.mongo_connector.push(
            key=self.test_key,
            value=self.test_value,
            priority='NORMAL',
        )

        self.assertEqual(self.mongo_connector.len(self.test_key), 1)

        returned_value = self.mongo_connector.pop(
            key=self.test_key,
        )

        self.assertEqual(self.test_value, returned_value)
        self.assertEqual(self.mongo_connector.len(self.test_key), 0)

        for i in range(100000):
            self.mongo_connector.push(
                key=self.test_key,
                value=self.test_value,
                priority='NORMAL',
            )
        self.assertEqual(self.mongo_connector.len(self.test_key), 100000)
        self.mongo_connector.delete(
            key=self.test_key,
        )
        self.assertEqual(self.mongo_connector.len(self.test_key), 0)

        self.mongo_connector.push_bulk(
            key=self.test_key,
            values=[self.test_value] * 100,
            priority='NORMAL',
        )
        self.assertEqual(self.mongo_connector.len(self.test_key), 100)
        values = self.mongo_connector.pop_bulk(
            key=self.test_key,
            count=100,
        )
        self.assertEqual(values, [self.test_value] * 100)
        self.assertEqual(self.mongo_connector.len(self.test_key), 0)

        added = self.mongo_connector.add_to_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(added)
        added = self.mongo_connector.add_to_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertFalse(added)
        is_member = self.mongo_connector.is_member_of_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(is_member)
        removed = self.mongo_connector.remove_from_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(removed)
        removed = self.mongo_connector.remove_from_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertFalse(removed)

    def test_connector_pickleability(
        self,
    ):
        pickled_connector = pickle.dumps(self.mongo_connector)
        pickled_connector = pickle.loads(pickled_connector)

        self.assertEqual(pickled_connector.len(self.test_key), 0)
        self.assertEqual(self.mongo_connector.len(self.test_key), 0)

        pickled_connector.push(
            key=self.test_key,
            value=self.test_value,
            priority='NORMAL',
        )

        self.assertEqual(pickled_connector.len(self.test_key), 1)
        self.assertEqual(self.mongo_connector.len(self.test_key), 1)

        returned_value = pickled_connector.pop(
            key=self.test_key,
        )

        self.assertEqual(self.test_value, returned_value)
        self.assertEqual(pickled_connector.len(self.test_key), 0)
        self.assertEqual(self.mongo_connector.len(self.test_key), 0)

        for i in range(10):
            pickled_connector.push(
                key=self.test_key,
                value=self.test_value,
                priority='NORMAL',
            )
        self.assertEqual(pickled_connector.len(self.test_key), 10)
        self.assertEqual(self.mongo_connector.len(self.test_key), 10)
        pickled_connector.delete(
            key=self.test_key,
        )
        self.assertEqual(pickled_connector.len(self.test_key), 0)
        self.assertEqual(self.mongo_connector.len(self.test_key), 0)

        pickled_connector.push_bulk(
            key=self.test_key,
            values=[self.test_value] * 100,
            priority='NORMAL',
        )
        self.assertEqual(pickled_connector.len(self.test_key), 100)
        self.assertEqual(self.mongo_connector.len(self.test_key), 100)
        values = pickled_connector.pop_bulk(
            key=self.test_key,
            count=100,
        )
        self.assertEqual(values, [self.test_value] * 100)
        self.assertEqual(pickled_connector.len(self.test_key), 0)
        self.assertEqual(self.mongo_connector.len(self.test_key), 0)

        added = pickled_connector.add_to_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(added)
        added = pickled_connector.add_to_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertFalse(added)
        is_member = pickled_connector.is_member_of_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(is_member)
        removed = pickled_connector.remove_from_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertTrue(removed)
        removed = pickled_connector.remove_from_set(
            set_name=self.set_name,
            value=self.test_set_value,
        )
        self.assertFalse(removed)
