from distutils.core import setup
setup(
    name='scrapy-rabbitmq-scheduler',
    version='0.1',
    description='A scrapy scheduler getting requests from a RabbitMQ queue using pika.',
    url='http://github.com/yall/scrapy-rabbitmq-scheduler',
    author='Jonathan Geslin',
    author_email='jonathan.geslin@gmail.com',
    license='MIT',
    py_modules=['scrapy_rabbitmq_scheduler'],
    install_requires=[
        'pika'
    ],
    zip_safe=False)
