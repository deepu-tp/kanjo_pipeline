class TweetProcessingError(Exception):
    pass


class DuplicateTweetError(TweetProcessingError):
    pass


class NonEnglishTweetError(TweetProcessingError):
    pass
