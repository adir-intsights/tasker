# tasker

[![Build Status](https://travis-ci.org/wavenator/tasker.svg?branch=master)](https://travis-ci.org/wavenator/tasker)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/eae56b505e034d9785d6bce47ed04355)](https://www.codacy.com/app/wavenator/tasker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=wavenator/tasker&amp;utm_campaign=Badge_Grade)

## Start redis servers

```shell
docker run \
    --interactive \
    --tty \
    --rm \
    --publish=6379:6379 \
    redis bash -c " \
        redis-server \
            --save '' \
            --protected-mode no \
            --bind 0.0.0.0 \
            --requirepass e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 \
    "
docker run \
    --interactive \
    --tty \
    --rm \
    --publish=6380:6379 \
    redis bash -c " \
        redis-server \
            --save '' \
            --protected-mode no \
            --bind 0.0.0.0 \
            --requirepass e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 \
    "
docker run \
    --interactive \
    --tty \
    --rm \
    --publish=27030:27017 \
    mongo
docker run \
    --interactive \
    --tty \
    --rm \
    --publish=50001:50001 \
    --volume "${PWD}":/tasker \
    tasker:tasker bash -c " \
        cd /tasker; \
        python3.6 -m tasker.server.server --port=50001 --database-path /tasker_db; \
    "
```

## Start a monitoring server

```shell
docker run \
    --interactive \
    --tty \
    --rm \
    --publish=9999:9999/udp \
    --publish=8080:8080 \
    python bash -c " \
        git clone -b master https://github.com/wavenator/tasker.git; \
        cd tasker; \
        python setup.py install; \
        python -m tasker.monitor.server \
        --redis-node 127.0.0.1 6379 e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 0 \
        --redis-node 127.0.0.1 6380 e082ebf6c7fff3997c4bb1cb64d6bdecd0351fa270402d98d35acceef07c6b97 0 \
        --udp-server 127.0.0.1 9999 \
        --web-server 127.0.0.1 8080 \
    "
```

## Run tests

```shell
docker run \
    --interactive \
    --tty \
    --rm \
    --net=host \
    --volume "${PWD}":/tasker \
    tasker:tasker bash -c " \
        cd /tasker; \
        pytest tasker/tests; \
    "

python3 -m unittest discover tasker.tests
```
