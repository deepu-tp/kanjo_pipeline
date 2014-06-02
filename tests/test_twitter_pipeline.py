from nose.tools import *
from celery.execute import send_task
import unittest
import os
import gzip
import json
from pprint import pprint
from kanjo_pipeline.celery_app import app


class TestTwitterPipeline(unittest.TestCase):

    def test_process_tweet(self):
        tweet_dir = 'tests/data/tweets/pepsi'
        files = os.listdir(tweet_dir)[:1]
        for _f in files:
            with gzip.open(os.path.join(tweet_dir, _f), 'rb') as f:
                for tweet in f:
                    tweet = json.loads(tweet)
                    tweet['query'] = 'pepsi'
                    app.send_task('kanjo_pipeline.twitter.tasks.process_tweet',
                                  args=[tweet])