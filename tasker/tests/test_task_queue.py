import unittest
import datetime
import threading
import time

from .. import task_queue
from .. import connector
from .. import queue
from .. import encoder


class TaskQueueTestCase:
    order_matters = True

    def test_purge_tasks(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=0,
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
        )
        self.test_task_queue.apply_async_one(
            task=task,
            priority='NORMAL',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=1,
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=0,
        )

    def test_number_of_enqueued_tasks(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=0,
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
        )
        self.test_task_queue.apply_async_one(
            task=task,
            priority='NORMAL',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=1,
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=0,
        )

        self.test_task_queue.apply_async_many(
            tasks=[task] * 100,
            priority='NORMAL',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=100,
        )
        self.test_task_queue.apply_async_many(
            tasks=[task] * 1000,
            priority='NORMAL',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=1100,
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=0,
        )

    def test_craft_task(
        self,
    ):
        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=False,
        )
        current_date = datetime.datetime.utcnow().timestamp()
        date = task.pop('date')
        self.assertAlmostEqual(
            first=date / (10 ** 8),
            second=current_date / (10 ** 8),
        )
        self.assertEqual(
            first=task,
            second={
                'name': 'test_task',
                'args': (),
                'kwargs': {},
                'run_count': 0,
                'completion_key': None,
            }
        )

        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(1, 2, 3),
            kwargs={
                'a': 1,
                'b': 2,
            },
            report_completion=True,
        )
        current_date = datetime.datetime.utcnow().timestamp()
        date = task.pop('date')
        self.assertAlmostEqual(
            first=date / (10 ** 8),
            second=current_date / (10 ** 8),
        )
        completion_key = task.pop('completion_key')
        self.assertNotEqual(
            first=completion_key,
            second=None,
        )
        self.assertEqual(
            first=task,
            second={
                'name': 'test_task',
                'args': (1, 2, 3),
                'kwargs': {
                    'a': 1,
                    'b': 2,
                },
                'run_count': 0,
            }
        )

    def test_report_complete(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=True,
        )
        completion_key = task['completion_key']
        self.assertTrue(
            expr=self.test_task_queue.queue.has_result(
                queue_name='test_task',
                result_id=completion_key,
            ),
        )
        self.test_task_queue.report_complete(
            task=task,
        )
        self.assertFalse(
            expr=self.test_task_queue.queue.has_result(
                queue_name='test_task',
                result_id=completion_key,
            )
        )

    def test_wait_task_finished(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=True,
        )
        report_complete_timer = threading.Timer(
            interval=2.0,
            function=self.test_task_queue.report_complete,
            args=(task,),
        )
        report_complete_timer.start()

        before = time.time()
        self.test_task_queue.wait_task_finished(
            task=task,
        )
        after = time.time()
        self.assertTrue(
            expr=3.0 > after - before > 2.0,
        )

    def test_wait_queue_empty(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=True,
        )
        self.test_task_queue.apply_async_one(
            task=task,
            priority='NORMAL',
        )
        purge_tasks_timer = threading.Timer(
            interval=2.0,
            function=self.test_task_queue.purge_tasks,
            args=('test_task',),
        )
        purge_tasks_timer.start()

        before = time.time()
        self.test_task_queue.wait_queue_empty(
            task_name='test_task',
        )
        after = time.time()
        self.assertTrue(
            expr=3.5 > after - before > 3.0,
        )

    def test_apply_async_one(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        task_two = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={
                'a': 1,
            },
            report_completion=True,
        )
        task_three = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={},
            report_completion=True,
        )

        self.test_task_queue.apply_async_one(
            task=task_one,
            priority='NORMAL',
        )
        self.test_task_queue.apply_async_one(
            task=task_two,
            priority='NORMAL',
        )
        self.test_task_queue.apply_async_one(
            task=task_three,
            priority='NORMAL',
        )
        task_one_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
            number_of_items=1,
        )[0]
        task_two_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
            number_of_items=1,
        )[0]
        task_three_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
            number_of_items=1,
        )[0]
        if self.order_matters:
            self.assertEqual(
                first=task_one,
                second=task_one_test,
            )
            self.assertEqual(
                first=task_two,
                second=task_two_test,
            )
            self.assertEqual(
                first=task_three,
                second=task_three_test,
            )
        else:
            self.assertIn(
                member=task_one,
                container=[task_one_test, task_two_test, task_three_test],
            )
            self.assertIn(
                member=task_two,
                container=[task_one_test, task_two_test, task_three_test],
            )
            self.assertIn(
                member=task_three,
                container=[task_one_test, task_two_test, task_three_test],
            )

        if self.order_matters:
            self.assertTrue(
                expr=self.test_task_queue.queue.has_result(
                    queue_name='test_task',
                    result_id=task_two['completion_key'],
                )
            )
            self.assertTrue(
                expr=self.test_task_queue.queue.has_result(
                    queue_name='test_task',
                    result_id=task_three['completion_key'],
                )
            )
        else:
            tasks_to_wait = [
                task_to_wait
                for task_to_wait in [task_one_test, task_two_test, task_three_test]
                if task_to_wait['completion_key'] is not None
            ]

            self.assertEqual(
                first=len(tasks_to_wait),
                second=2,
            )

            for task_to_wait in tasks_to_wait:
                self.assertTrue(
                    expr=self.test_task_queue.queue.has_result(
                        queue_name='test_task',
                        result_id=task_to_wait['completion_key'],
                    )
                )

    def test_apply_async_many(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task_one',
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task_two',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task_one',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        task_two = self.test_task_queue.craft_task(
            task_name='test_task_one',
            args=(),
            kwargs={
                'a': 1,
            },
            report_completion=True,
        )
        task_three = self.test_task_queue.craft_task(
            task_name='test_task_two',
            args=(),
            kwargs={},
            report_completion=True,
        )
        self.test_task_queue.apply_async_many(
            tasks=[
                task_one,
                task_two,
                task_three,
            ],
            priority='NORMAL',
        )
        task_one_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task_one',
            number_of_items=1,
        )[0]
        task_two_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task_one',
            number_of_items=1,
        )[0]
        task_three_test = self.test_task_queue.queue.dequeue(
            queue_name='test_task_two',
            number_of_items=1,
        )[0]

        if self.order_matters:
            self.assertEqual(
                first=task_one,
                second=task_one_test,
            )
            self.assertEqual(
                first=task_two,
                second=task_two_test,
            )
            self.assertEqual(
                first=task_three,
                second=task_three_test,
            )
        else:
            self.assertIn(
                member=task_one,
                container=[task_one_test, task_two_test],
            )
            self.assertIn(
                member=task_two,
                container=[task_one_test, task_two_test],
            )
            self.assertEqual(
                first=task_three,
                second=task_three_test,
            )

        if self.order_matters:
            self.assertTrue(
                expr=self.test_task_queue.queue.has_result(
                    queue_name='test_task_one',
                    result_id=task_two['completion_key'],
                )
            )
            self.assertTrue(
                expr=self.test_task_queue.queue.has_result(
                    queue_name='test_task_two',
                    result_id=task_three['completion_key'],
                )
            )
        else:
            tasks_to_wait = [
                task_to_wait
                for task_to_wait in [task_one_test, task_two_test, task_three_test]
                if task_to_wait['completion_key'] is not None
            ]

            self.assertEqual(
                first=len(tasks_to_wait),
                second=2,
            )

            for task_to_wait in tasks_to_wait:
                self.assertTrue(
                    expr=self.test_task_queue.queue.has_result(
                        queue_name=task_to_wait['name'],
                        result_id=task_to_wait['completion_key'],
                    )
                )

        self.assertEqual(
            first=task_one,
            second=task_one_test,
        )
        self.assertEqual(
            first=task_two,
            second=task_two_test,
        )
        self.assertEqual(
            first=task_three,
            second=task_three_test,
        )

        self.assertTrue(
            expr=self.test_task_queue.queue.has_result(
                queue_name='test_task_one',
                result_id=task_two['completion_key'],
            )
        )
        self.assertTrue(
            expr=self.test_task_queue.queue.has_result(
                queue_name='test_task_two',
                result_id=task_three['completion_key'],
            )
        )

    def test_queue_priority(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task_NORMAL_priority = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(1,),
            kwargs={
                'priority': 'NORMAL',
            },
            report_completion=False,
        )
        task_HIGH_priority = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(),
            kwargs={
                'priority': 'HIGH',
            },
            report_completion=False,
        )
        self.test_task_queue.apply_async_one(
            task=task_NORMAL_priority,
            priority='NORMAL',
        )
        self.test_task_queue.apply_async_one(
            task=task_NORMAL_priority,
            priority='NORMAL',
        )
        self.test_task_queue.apply_async_one(
            task=task_HIGH_priority,
            priority='HIGH',
        )
        self.test_task_queue.apply_async_one(
            task=task_HIGH_priority,
            priority='HIGH',
        )
        self.test_task_queue.apply_async_one(
            task=task_NORMAL_priority,
            priority='NORMAL',
        )
        self.test_task_queue.apply_async_one(
            task=task_NORMAL_priority,
            priority='NORMAL',
        )
        self.test_task_queue.apply_async_one(
            task=task_HIGH_priority,
            priority='HIGH',
        )
        self.test_task_queue.apply_async_one(
            task=task_HIGH_priority,
            priority='HIGH',
        )
        self.assertEqual(
            first=self.test_task_queue.number_of_enqueued_tasks(
                task_name='test_task',
            ),
            second=8,
        )

        high_priority_tasks = self.test_task_queue.get_tasks(
            task_name='test_task',
            number_of_tasks=2,
        )
        high_priority_tasks += self.test_task_queue.get_tasks(
            task_name='test_task',
            number_of_tasks=2,
        )
        low_priority_tasks = self.test_task_queue.get_tasks(
            task_name='test_task',
            number_of_tasks=2,
        )
        low_priority_tasks += self.test_task_queue.get_tasks(
            task_name='test_task',
            number_of_tasks=2,
        )
        low_priority_tasks += self.test_task_queue.get_tasks(
            task_name='test_task',
            number_of_tasks=2,
        )
        self.assertEqual(
            first=[task_HIGH_priority['kwargs']['priority']] * 4,
            second=[task['kwargs']['priority'] for task in high_priority_tasks],
        )
        self.assertEqual(
            first=[task_NORMAL_priority['kwargs']['priority']] * 4,
            second=[task['kwargs']['priority'] for task in low_priority_tasks],
        )

    def test_get_tasks(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task_one',
        )
        self.test_task_queue.purge_tasks(
            task_name='test_task_two',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task_one',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        task_two = self.test_task_queue.craft_task(
            task_name='test_task_one',
            args=(),
            kwargs={
                'a': 1,
            },
            report_completion=True,
        )
        task_three = self.test_task_queue.craft_task(
            task_name='test_task_two',
            args=(),
            kwargs={},
            report_completion=True,
        )
        self.test_task_queue.apply_async_many(
            tasks=[
                task_one,
                task_two,
                task_three,
            ],
            priority='NORMAL',
        )
        tasks_one = self.test_task_queue.get_tasks(
            task_name='test_task_one',
            number_of_tasks=3,
        )
        tasks_two = self.test_task_queue.get_tasks(
            task_name='test_task_two',
            number_of_tasks=1,
        )
        self.assertIn(
            member=task_one,
            container=tasks_one,
        )
        self.assertIn(
            member=task_two,
            container=tasks_one,
        )
        self.assertIn(
            member=task_three,
            container=tasks_two,
        )

    def test_retry(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        self.assertEqual(task_one['run_count'], 0)
        self.test_task_queue.apply_async_one(
            task=task_one,
            priority='NORMAL',
        )
        task_one = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
            number_of_items=1,
        )[0]

        self.test_task_queue.retry(
            task=task_one,
        )
        task_one = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
            number_of_items=1,
        )[0]
        self.assertEqual(
            first=task_one['run_count'],
            second=1,
        )

    def test_requeue(
        self,
    ):
        self.test_task_queue.purge_tasks(
            task_name='test_task',
        )
        task_one = self.test_task_queue.craft_task(
            task_name='test_task',
            args=(1,),
            kwargs={},
            report_completion=False,
        )
        self.assertEqual(task_one['run_count'], 0)
        self.test_task_queue.apply_async_one(
            task=task_one,
            priority='NORMAL',
        )
        task_one = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
            number_of_items=1,
        )[0]

        self.test_task_queue.requeue(
            task=task_one,
        )
        task_one = self.test_task_queue.queue.dequeue(
            queue_name='test_task',
            number_of_items=1,
        )[0]
        self.assertEqual(
            first=task_one['run_count'],
            second=0,
        )


class RedisSingleServerTaskQueueTestCase(
    TaskQueueTestCase,
    unittest.TestCase,
):
    order_matters = True

    def setUp(
        self,
    ):
        redis_cluster_connector = connector.redis_cluster.Connector(
            nodes=[
                {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                    'database': 0,
                },
            ]
        )

        test_queue = queue.Queue(
            connector=redis_cluster_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_task_queue = task_queue.TaskQueue(
            queue=test_queue,
        )


class RedisClusterSingleServerTaskQueueTestCase(
    TaskQueueTestCase,
    unittest.TestCase,
):
    order_matters = True

    def setUp(
        self,
    ):
        redis_cluster_connector = connector.redis_cluster.Connector(
            nodes=[
                {
                    'host': '127.0.0.1',
                    'port': 6379,
                    'password': 'e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97',
                    'database': 0,
                },
            ]
        )

        test_queue = queue.Queue(
            connector=redis_cluster_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_task_queue = task_queue.TaskQueue(
            queue=test_queue,
        )


class RedisClusterMultipleServerTaskQueueTestCase(
    TaskQueueTestCase,
    unittest.TestCase,
):
    order_matters = False

    def setUp(
        self,
    ):
        redis_cluster_connector = connector.redis_cluster.Connector(
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

        test_queue = queue.Queue(
            connector=redis_cluster_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_task_queue = task_queue.TaskQueue(
            queue=test_queue,
        )


class MongoTaskQueueTestCase(
    TaskQueueTestCase,
    unittest.TestCase,
):
    order_matters = True

    def setUp(
        self,
    ):
        mongo_connector = connector.mongo.Connector(
            mongodb_uri='mongodb://localhost:27030/',
        )

        test_queue = queue.Queue(
            connector=mongo_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_task_queue = task_queue.TaskQueue(
            queue=test_queue,
        )


class TaskerServerTaskQueueTestCase(
    TaskQueueTestCase,
    unittest.TestCase,
):
    order_matters = True

    def setUp(
        self,
    ):
        tasker_server_connector = connector.tasker.Connector(
            host='127.0.0.1',
            port=50001,
        )

        test_queue = queue.Queue(
            connector=tasker_server_connector,
            encoder=encoder.encoder.Encoder(
                compressor_name='dummy',
                serializer_name='pickle',
            ),
        )
        self.test_task_queue = task_queue.TaskQueue(
            queue=test_queue,
        )
