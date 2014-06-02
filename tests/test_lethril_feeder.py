from nose.tools import *
from celery.execute import send_task
import unittest
import os
import glob
import shutil
import gzip
import json
from pprint import pprint
from kanjo_pipeline.twitter.feeder import LethrilFeeder
from kanjo_pipeline.celery_app import app
import logging
import sys

log = logging.getLogger('kanjo_pipeline.twitter.feeder')
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(stream=sys.stdout))

class TestLethrilFeeder(unittest.TestCase):

    def test_run(self):
        tweet_dir = 'tests/data/tweets'
        sleep_period = 10
        f = LethrilFeeder(tweet_dir, sleep_period, 1000)
        try:
            f.run()
        except KeyboardInterrupt:
            pass

        fs = glob.glob('tests/data/tweets/*/done/*')
        for f in fs:
            fname = os.path.basename(f)
            pardir = f.split('/done/')[0]
            dst = os.path.join(pardir, fname)
            shutil.move(f, dst)

        print app.conf.get_by_parts('BROKER', 'URL')
