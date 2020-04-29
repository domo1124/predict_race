import base64
import os
import sys
import time
import twitter

def get_twitter_api(credentials):
    return twitter.Api(
        consumer_key=credentials['consumer_key'],
        consumer_secret=credentials['consumer_secret'],
        access_token_key=credentials['access_token_key'],
        access_token_secret=credentials['access_token_secret'])

def twitter_output(event, context):

    msg = base64.b64decode(event['data']).decode('utf-8')
    try:
        api = get_twitter_api(os.environ)
        status = api.PostUpdate(msg)
        print(str(status))
    except:
        print(str(sys.exc_info()))
    return 'Done'