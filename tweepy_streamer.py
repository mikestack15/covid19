from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import twitter_credentials


class TwitterStreamer():
    """
    Class for streaming and processing live tweets
    """

    def stream_tweets(self,fetched_tweets_filename,hash_tag_list):
        #this handles twitter authentication and connection to the twitter streaming api
        listener = StdOutListener()
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)

        stream = Stream(auth, listener)

        stream.filter(track=hash_tag_list)



class StdOutListener(StreamListener):
    """
    This a basic listener class that just prints received tweets to stdout
    """
    def __init__(self,fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        print(data)
        return True

    def on_error(self,status):
        print(status)

if __name__ == "__main__":

