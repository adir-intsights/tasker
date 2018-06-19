import tasker
import time
import socket


class Worker(
    tasker.worker.Worker,
):
    name = 'test_worker'
    config = {
        'encoder': {
            'compressor': 'dummy',
            'serializer': 'pickle',
        },
        'monitoring': {
            'host_name': socket.gethostname(),
            'stats_server': {
                'host': 'localhost',
                'port': 9999,
            }
        },
        'connector': {
            'type': 'tasker',
            'params': {
                'host': 'localhost',
                'port': 50001,
            },
        },
        'timeouts': {
            'soft_timeout': 3.0,
            'hard_timeout': 35.0,
            'critical_timeout': 0.0,
            'global_timeout': 0.0,
        },
        'limits': {
            'memory': 0,
        },
        'executor': {
            'type': 'serial',
        },
        'profiler': {
            'enabled': True,
            'num_of_slowest_methods_to_log': 50,
        },
        'max_tasks_per_run': 1000000,
        'tasks_per_transaction': 25000,
        'max_retries': 3,
        'report_completion': False,
        'heartbeat_interval': 10.0,
    }

    def init(
        self,
    ):
        pass

    def work(
        self,
        run_type,
    ):
        if run_type == 'start':
            self.logger.error('start    : {}'.format(time.time()))
        elif run_type == 'end':
            self.logger.error('end      : {}'.format(time.time()))


def main():
    worker = Worker()
    worker.init_worker()

    worker.apply_async_one(
        run_type='start',
    )

    before_push = time.time()
    for i in range(1):
        for i in range(5):
            tasks = []
            for j in range(20000):
                task_obj = worker.craft_task(
                    run_type='',
                )
                tasks.append(task_obj)
            worker.apply_async_many(
                tasks=tasks,
            )
    after_push = time.time()
    print('pushing time: {}'.format(after_push - before_push))

    worker.apply_async_one(
        run_type='end',
    )

    supervisor = tasker.supervisor.Supervisor(
        worker_class=Worker,
        concurrent_workers=4,
    )
    supervisor.start()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        print('killed')
