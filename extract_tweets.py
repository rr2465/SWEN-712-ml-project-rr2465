from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor

consumer_key = "K9oxLua3j9edH4YjiWzwXPmHg" #twitter app's API Keys
consumer_secret = "aigQPUAEbiPgcjNPWEZh6iPoxh1Hi82ENPlTH6jqiesdqrApfn" #twitter appâ€™s API secret Key
access_token = "1227239991434194944-eKwWQI4a3A4fYwof2eWZ28vp7bQv8F" #twitter appâ€™s Access token
access_token_secret = "IfMedS595WuPP7FXSKqaIzRW9XygVcqbgOG2G3UNbZoIc" #twitter appâ€™s access token secret

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
auth_api = API(auth)

t_tweets = auth_api.user_timeline(screen_name = 'IBMAccess', count = 777, include_rts = False, tweet_mode = 'extended')

final_tweets = [each_tweet.full_text for each_tweet in t_tweets]

with open('/dbfs/t_tweets.txt', 'w') as f:
  for item in final_tweets:
    f.write("%s\n" %item)

read_tweets = []
with open('/dbfs/t_tweets.txt','r') as f:
  read_tweets.append(f.read())

for x in read_tweets:
  print(x)
