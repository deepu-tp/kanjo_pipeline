from kanjo.twitter.utils import (
    enrich_geo,
    preprocess_tweet,
    preprocess_tweet_text
)

from kanjo.utils import import_from_string

from kanjo.utils.lexicon_utils import (
    emoticon_polarity,
    hashtag_polarity,
    is_english,
    afinn_polarity
)

from kanjo.classifiers.stanford_corenlp import StanfordNLPSentiment
from kanjo.classifiers.sentiment140 import Sentiment140
from kanjo.classifiers.neutral_classifier import NeutralClassifier
from kanjo.classifiers.sentiment_classifier import SentimentClassifier

from collections import deque
import translitcodec
from sklearn.externals import joblib
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from dateutil.parser import parse as date_parse
from calendar import timegm
from kanjo_pipeline.twitter.errors import (
    DuplicateTweetError,
    TweetProcessingError,
    NonEnglishTweetError
)

class TweetProcessor(object):

    def __init__(self, stanford_nlp_url, sentiment140_appid,
                 neutral_classifier_config, sentiment_classifier_config,
                 mongo_uri, db):
        self.nlp_client = StanfordNLPSentiment(stanford_nlp_url)
        self.sentiment140_client = Sentiment140(sentiment140_appid)

        self.neutral_classifier = self.load_model(neutral_classifier_config)
        self.sentiment_classifier = self.load_model(
                                        sentiment_classifier_config
                                    )

        self.client = MongoClient(mongo_uri)
        self.db = self.client[db]
        self.tweets = self.db['tweets']
        self.aggregates = self.db['aggregates']

        # Ensure indexes
        self.tweets.ensure_index([('id', ASCENDING),
                                  ('query', ASCENDING)], unique=True,
                                    drop_dups=True)

        self.aggregates.ensure_index([('hour', ASCENDING),
                                      ])

    def load_model(self, config):
        model_class = import_from_string(config['model_class'])
        args = config.get('args', [])
        kwargs = config.get('kwargs', {})
        return model_class(*args, **kwargs)


    def process_tweet(self, tweet):
        # Discard tweet if we've seen it already for this query

        if self.tweets.find_one({'id' : tweet['id'],
                                'query' : tweet['query']}):
            raise DuplicateTweetError()

        # Process tweet text
        data = {}
        data['scores'] = {}

        c_text = preprocess_tweet(tweet)
        data['text'] = c_text
        data['preprocessed_text'] = preprocess_tweet_text(tweet['text'])

        # Check if tweet is primarily english
        data['is_english'] = is_english(c_text)
        if not data['is_english']:
            raise NonEnglishTweetError()

        # Get various polarity scores
        data['scores']['hashtag'] = hashtag_polarity(tweet['text'],
                                           verbose=True)
        data['scores']['emoticon'] = emoticon_polarity(tweet['text'],
                                             verbose=True)
        data['scores']['afinn'] = afinn_polarity(data['preprocessed_text'],
                                                 verbose=True)

        # External classifiers
        try:
            data['scores']['stanford_corenlp'] = self.nlp_client.classify(c_text,
                                                            verbose=True)
        except:
            raise TweetProcessingError(("Retrieving Stanford CoreNLP Score "
                                        "failed"))

        # Enriched geo from tweet
        data['geo'] = enrich_geo(tweet)

        tweet['processed'] = data

        return tweet

    def _compress_score_namespace(self, processed):
        feature_dict = {}
        for namespace, scores in processed.iteritems():
            for key, value in scores.iteritems():
                compressed_name = '_'.join([namespace, key])
                feature_dict[compressed_name] = value
        return feature_dict


    def is_neutral(self, tweet):
        feature_dict = self._compress_score_namespace(
                        tweet['processed']['scores'])
        score = self.neutral_classifier.classify(feature_dict,
                                                     verbose=True)
        return bool(score['polarity_score'])


    def get_sentiment(self, tweet):
        feature_dict = self._compress_score_namespace(
                        tweet['processed']['scores'])
        score = self.sentiment_classifier.classify(feature_dict,
                                                 verbose=True)
        return score


    def _get_geo_string(self, geo):
        parts = []
        for part in ['world', 'country', 'region', 'division',
                     'state', 'city']:
            try:
                parts.append(geo[part])
            except KeyError:
                break
        return ':::'.join(parts)


    def save_tweet(self, tweet):
        data = tweet['processed']
        data['id'] = tweet['id']
        data['query'] = tweet['query']

        metadata = tweet.get('_metadata', None)
        if metadata:
            data['metadata'] = metadata
        data['geo_string'] = self._get_geo_string(data['geo'])

        if data['failed']:
            sentiment = 'UNKNOWN'

        elif data['is_neutral']:
            sentiment = 'Neutral'

        else:
            sentiment = data['sentiment']['polarity']

        data['polarity'] = sentiment

        try:
            self.tweets.insert(data)
        except DuplicateKeyError:
            raise DuplicateTweetError("DUPLICATE TWEET : %s" % data['id'])
            return

        # Aggregate
        agg = {}
        created_at = date_parse(tweet['created_at'])
        agg['hour'] = timegm(created_at.timetuple()) // 3600
        agg['query'] = data['query']
        agg['geo'] = data['geo_string']

        self.aggregates.update(agg, {'$inc' : {sentiment : 1}}, upsert=True)