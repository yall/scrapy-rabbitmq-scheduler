# coding:utf-8

import os
import json
import logging
from os.path import join, exists

from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.misc import load_object
from scrapy.utils.job import job_dir
from scrapy import Request

import pika

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

class RabbitMQScheduler(object):

    def __init__(self, dupefilter, jobdir=None, dqclass=None, mqclass=None,
                 logunser=False, stats=None, pqclass=None, 
                 rabbitmq_queue_name=None, rabbitmq_url=None):
        self.df = dupefilter
        self.dqdir = self._dqdir(jobdir)
        self.pqclass = pqclass
        self.dqclass = dqclass
        self.mqclass = mqclass
        self.logunser = logunser
        self.stats = stats
        self.closing = False

        self.rabbitmq_queue_name = rabbitmq_queue_name
        self.rabbitmq_url = rabbitmq_url


    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        dupefilter_cls = load_object(settings['DUPEFILTER_CLASS'])
        dupefilter = dupefilter_cls.from_settings(settings)
        pqclass = load_object(settings['SCHEDULER_PRIORITY_QUEUE'])
        dqclass = load_object(settings['SCHEDULER_DISK_QUEUE'])
        mqclass = load_object(settings['SCHEDULER_MEMORY_QUEUE'])
        logunser = settings.getbool('LOG_UNSERIALIZABLE_REQUESTS', settings.getbool('SCHEDULER_DEBUG'))

        rabbitmq_queue_name = settings.get('RABBITMQ_INPUT_QUEUE_NAME')
        rabbitmq_url = settings.get('RABBITMQ_URL')

        return cls(dupefilter, jobdir=job_dir(settings), logunser=logunser,
                   stats=crawler.stats, pqclass=pqclass, dqclass=dqclass, mqclass=mqclass,
                   rabbitmq_queue_name=rabbitmq_queue_name, rabbitmq_url=rabbitmq_url)

    def has_pending_requests(self):
        return not self.closing or len(self) > 0

    def open(self, spider):
        self.spider = spider
        self.mqs = self.pqclass(self._newmq)
        self.dqs = self._dq() if self.dqdir else None

        self.connection = pika.BlockingConnection(pika.URLParameters(self.rabbitmq_url))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.rabbitmq_queue_name, durable=True)

        return self.df.open()

    def close(self, reason):
        if self.dqs:
            prios = self.dqs.close()
            with open(join(self.dqdir, 'active.json'), 'w') as f:
                json.dump(prios, f)
        return self.df.close(reason)

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        dqok = self._dqpush(request)
        if dqok:
            self.stats.inc_value('scheduler/enqueued/disk', spider=self.spider)
        else:
            self._mqpush(request)
            self.stats.inc_value('scheduler/enqueued/memory', spider=self.spider)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True

    def build_rabbitmq_request(self, method_frame, header_frame, body):
        url = body.decode()
        return Request(url, dont_filter=True)

    def next_request(self):
        request = self.mqs.pop()
        if request:
            self.stats.inc_value('scheduler/dequeued/memory', spider=self.spider)
        
        if not request:
            request = self._dqpop()
            if request:
                self.stats.inc_value('scheduler/dequeued/disk', spider=self.spider)

        if not request:
            try:
                method_frame, header_frame, body = self.channel.basic_get(self.rabbitmq_queue_name, no_ack=True)
        
                if body:
                    if hasattr(self.spider, 'build_rabbitmq_request'):
                        request = self.spider.build_rabbitmq_request(method_frame, header_frame, body)
                    else:
                        request = self.build_rabbitmq_request(method_frame, header_frame, body)

            except pika.exceptions.ChannelClosed:
                logger.warning("Channel closed, try to reopen queue")
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.rabbitmq_queue_name, durable=True)

            if request:
                self.stats.inc_value('scheduler/enqueued', spider=self.spider)
                self.stats.inc_value('scheduler/enqueued/rabbitmq', spider=self.spider)
                self.stats.inc_value('scheduler/dequeued/rabbitmq', spider=self.spider)

        if request:
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)

        return request

    def __len__(self):
        return len(self.dqs) + len(self.mqs) if self.dqs else len(self.mqs)

    def _dqpush(self, request):
        if self.dqs is None:
            return
        try:
            reqd = request_to_dict(request, self.spider)
            self.dqs.push(reqd, -request.priority)
        except ValueError as e:  # non serializable request
            if self.logunser:
                msg = ("Unable to serialize request: %(request)s - reason:"
                       " %(reason)s - no more unserializable requests will be"
                       " logged (stats being collected)")
                logger.warning(msg, {'request': request, 'reason': e},
                               exc_info=True, extra={'spider': self.spider})
                self.logunser = False
            self.stats.inc_value('scheduler/unserializable',
                                 spider=self.spider)
            return
        else:
            return True

    def _mqpush(self, request):
        self.mqs.push(request, -request.priority)

    def _dqpop(self):
        if self.dqs:
            d = self.dqs.pop()
            if d:
                return request_from_dict(d, self.spider)

    def _newmq(self, priority):
        return self.mqclass()

    def _newdq(self, priority):
        return self.dqclass(join(self.dqdir, 'p%s' % priority))

    def _dq(self):
        activef = join(self.dqdir, 'active.json')
        if exists(activef):
            with open(activef) as f:
                prios = json.load(f)
        else:
            prios = ()
        q = self.pqclass(self._newdq, startprios=prios)
        if q:
            logger.info("Resuming crawl (%(queuesize)d requests scheduled)",
                        {'queuesize': len(q)}, extra={'spider': self.spider})
        return q

    def _dqdir(self, jobdir):
        if jobdir:
            dqdir = join(jobdir, 'requests.queue')
            if not exists(dqdir):
                os.makedirs(dqdir)
            return dqdir