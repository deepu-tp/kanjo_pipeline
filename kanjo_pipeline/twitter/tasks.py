from celery.utils.log import get_task_logger
from kanjo_pipeline.celery_app import app
from celery import Task
from kanjo_pipeline.twitter.tweet_processor import TweetProcessor
from celery.contrib.batches import Batches
from celery import chain
from kanjo_pipeline.twitter.errors import (
    DuplicateTweetError,
    TweetProcessingError,
    NonEnglishTweetError
)


log = get_task_logger(__name__)

class TweetProcessorTask(Task):
    abstract = True
    _processor = None

    @property
    def processor(self):
        if self._processor is None:
            url = app.conf.get_by_parts('STANFORD_CORENLP', 'URL')
            appid = app.conf.get_by_parts('SENTIMENT_140', 'APPID')
            n_clsf = app.conf.get_by_parts('CLASSIFIER', 'NEUTRAL')
            s_clsf = app.conf.get_by_parts('CLASSIFIER', 'SENTIMENT')
            mongo_uri = app.conf.get_by_parts('MONGO_DB', 'URI')
            mongo_db = app.conf.get_by_parts('MONGO_DB', 'DB')
            self._processor = TweetProcessor(url, appid, n_clsf, s_clsf,
                                             mongo_uri, mongo_db)
        return self._processor

class Sentiment140Task(Batches, TweetProcessorTask):
    pass


@app.task(base=TweetProcessorTask, ignore_result=True)
def classify_tweet(tweet):
    self = classify_tweet
    tweet['processed']['failed'] = False

    if not tweet['processed']['is_english']:
        # Tweet is not in english
        log.warn("NOT IN EN : %s", tweet['processed']['text'])
        return

    try:
        is_neutral = self.processor.is_neutral(tweet)
    except KeyError:
        tweet['processed']['failed'] = True

    else:
        tweet['processed']['is_neutral'] = is_neutral

        if not is_neutral:
            try:
                sentiment = self.processor.get_sentiment(tweet)
            except KeyError:
                tweet['processed']['failed'] = True 
            else:
                tweet['processed']['sentiment'] = sentiment

    # Save tweet
    self.processor.save_tweet(tweet)


@app.task(base=Sentiment140Task, flush_every=5000, flush_interval=600,
          ignore_result=True)
def sentiment_140(requests):    
    log.warn("s140")
    self = sentiment_140
    data = []
    tweets_lkp = {}
    requests = list(requests)
    failed = False
    for i, request in enumerate(requests):
        tweet = request.args[0]
        rec = {
                'id' : tweet['id'],
                'text' : tweet['processed']['text']
              }
        query = tweet.get('query', '')
        if query and query != 'N/A':
            rec['query'] = query
        data.append(rec)
        tweets_lkp[tweet['id']] = i

    try:
        results = self.processor.sentiment140_client.bulk_classify(data,
                                                          verbose=True)
    except Exception as e:
        log.error("Error while getting SENTIMENT 140 SCORES")
        log.exception(e)
        failed = True

    for result in results:
        tweet = requests[tweets_lkp[result['id']]].args[0]
        callback = requests[tweets_lkp[result['id']]].args[1]
        del result['id']
        del result['text']
        if not failed:
            tweet['processed']['scores']['sentiment140'] = result

        callback(tweet)


@app.task(base=TweetProcessorTask, ignore_result=True)
def process_tweet(tweet):
    self = process_tweet
    try:
        tweet = self.processor.process_tweet(tweet)
    except DuplicateTweetError as e:
        log.warn("Duplicate Tweet : %s", tweet['id'])

    except NonEnglishTweetError as e:
        log.warn("Tweet not in english : %s", tweet['id'])

    except TweetProcessingError as e:
        log.warn(e)

    else:
        return sentiment_140.delay(tweet, chain(classify_tweet.s()))