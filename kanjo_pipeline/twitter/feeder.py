import glob
import json
import logging
import os
import shutil
import time
import gzip
import redis
from kanjo_pipeline.celery_app import app

log = logging.getLogger(__name__)


class LethrilFeeder():

    def __init__(self, tweet_dir, sleep_period, max_queue_size):
        self.tweet_dir = tweet_dir
        self.sleep_period = sleep_period
        self.running = False
        self.max_queue_size = max_queue_size

        host_port_db = app.conf.get_by_parts('BROKER', 'URL')
        host_port_db = host_port_db.split('redis://')[-1]
        host_port, db = host_port_db.split('/')
        host, port = host_port.split(':')
        self.broker = redis.Redis(host=host, port=int(port), db=int(db))


    @property
    def queue_size(self):
        length = self.broker.hlen('unacked') + self.broker.llen('celery')
        log.warn(length)
        return length


    def get_latest_file(self):
        files = glob.glob(self.tweet_dir + '/*/*.gz')
        try:
            return sorted(files, key=lambda x: 
                x.split(os.path.sep)[-1].split('_')[-1], reverse=True)[0]
        except IndexError:
            return None


    def feed(self, path):
        name = os.path.basename(path)
        query = os.path.dirname(path).split(os.path.sep)[-1]
        with gzip.open(path, 'rb') as f:
            for index, line in enumerate(f):
                tweet = json.loads(line.strip())
                metadata = {}
                metadata['file_name'] = name
                metadata['index'] = index
                tweet['_metadata'] = metadata
                tweet['query'] = query
                app.send_task('kanjo_pipeline.twitter.tasks.process_tweet',
                              args=[tweet])


    def _move_to_done(self, path):
        fname = os.path.basename(path)
        dst = path.replace(fname, os.path.join('done', fname))
        try:
            shutil.move(path, dst)
        except IOError as e:
            if e.errno == 2:
                os.mkdir(os.path.dirname(dst))
            else:
                raise e

    @property
    def can_send_task(self):
        return all((
            self.queue_size < self.max_queue_size,
            self.running
        ))


    def run(self):
        self.running = True
        while True:
            path = self.get_latest_file()
            can_send_task = self.can_send_task
            if path and can_send_task:
                try:
                    self.feed(path)
                except Exception as e:
                    log.error(e)
                else:
                    self._move_to_done(path)
            else:
                log.warn('%s : %s', can_send_task, path)

            if self.running:
                time.sleep(self.sleep_period)

            else:
                break