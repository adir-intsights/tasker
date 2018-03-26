import setuptools


setuptools.setup(
    name='tasker',
    version='0.4.0',
    author='gal@intsights.com',
    author_email='gal@intsights.com',
    description=('A fast, simple, task distribution library'),
    zip_safe=True,
    install_requires=[
        'aiohttp',
        'aioredis',
        'hiredis',
        'japronto',
        'msgpack-python',
        'psutil',
        'pymongo',
        'python-rocksdb',
        'redis',
        'requests',
        'uvloop==0.8.1',
    ],
    packages=setuptools.find_packages(),
    package_data={
        '': [
            '*.tpl',
        ],
    },
    include_package_data=True,
)
