CELERY_ACCEPT_CONTENT = ['pickle', 'json']

# Redis Broker
BROKER_URL = 'redis://localhost:6379/0'

CELERY_INCLUDE = ['kanjo_pipeline.twitter.tasks']

# CELERY_ALWAYS_EAGER = True

CELERY_TIMEZONE = 'UTC'

CELERYD_PREFETCH_MULTIPLIER = 0
CELERYD_CONCURRENCY = 4



STANFORD_CORENLP_URL = 'http://localhost:8080'
SENTIMENT_140_APPID = '<aapid>'

MONGO_DB_URI = 'mongodb://localhost:27017'
MONGO_DB_DB = 'kanjo'

CLASSIFIER_NEUTRAL = {
    'model_class' : 'kanjo.classifiers.bagging_classifier:BaggingClassifier',
    'kwargs' : {
        'model_configs' : [
            {
                'loader' : 'kanjo.classifiers.neutral_classifier:loader',
                'args' : ['models/neutral_classifiers/sanders_extratrees_entropy_v0.1/model/model']
            },

            {
                'loader' : 'kanjo.classifiers.neutral_classifier:loader',
                'args' : ['models/neutral_classifiers/sanders_extratrees_gini_v0.1/model/model']
            },

            {
                'loader' : 'kanjo.classifiers.bagging_classifier:loader',
                'args' : ['models/neutral_classifiers/sanders_svm_rbf_v0.1/model/model']
            },

        ]

    }
}

CLASSIFIER_SENTIMENT = {
    'model_class' : 'kanjo.classifiers.bagging_classifier:BaggingClassifier',
    'kwargs' : {
        'model_configs' : [
            {
                'loader' : 'kanjo.classifiers.sentiment_classifier:loader',
                'args' : ['models/sentiment_classifiers/stanford_extratrees_entropy_v0.1/model/model']
            },

            {
                'loader' : 'kanjo.classifiers.bagging_classifier:loader',
                'args' : ['models/sentiment_classifiers/stanford_svm_rbf_v0.1/model/model']
            },

        ]

    }
}

CELERY_ROUTES = {
        'kanjo_pipeline.twitter.tasks.classify_tweet': {
            'queue': 'classify_task'
        },
}