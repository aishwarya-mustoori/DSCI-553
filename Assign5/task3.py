import random
import sys
import tweepy
from collections import defaultdict


class StreamListener(tweepy.StreamListener):
    def __init__(self):
        self.api = api
        self.dictionary = defaultdict(int)
        self.sampling = []
        self.sequence = 0
        self.tagNumber = 0
        
    def get_tags(self, tags):
        all_tags = []
        for tag in tags:
            if len(tag) > 0 and tag['text'] !=None and tag['text'].encode('UTF-8').isalpha():
                all_tags.append(tag['text'])
                self.dictionary[tag['text']] += 1
        return all_tags

    def showOutput(self):
        #Print the 3 frequent tags
        sortValues = set(self.dictionary.values())
        sortCount = sorted(list(sortValues), reverse=True)
        if(len(sortCount)>=3):
            thirdMostFrequentWord = sortCount[2]
        else : 
            thirdMostFrequentWord = sortCount[-1]
        result = []
        for k, v in self.dictionary.items():
            if(v >= thirdMostFrequentWord):
                result.append([k, -v])
        result = sorted(result,key=lambda x : (x[1],x[0]))
        f = open(sys.argv[2], "a")
        f.write("The number of tweets with tags from the beginning: " +
                    str(self.sequence))
        f.write("\n")
        
        for frequency in result:
            f.write(str(frequency[0])+" : " + str(-frequency[1]))
            f.write("\n")
        f.write("\n")
        f.close()

    def on_status(self, status):
        hashtags = status.entities["hashtags"]
        all_tags = self.get_tags(hashtags)
        if(self.tagNumber + len(all_tags) < 100):
            if (len(all_tags) > 0):
                # There are some tags in the stream
                self.sequence += 1
                self.sampling += all_tags
                self.showOutput()
                self.tagNumber += len(all_tags)
        else:
            # Capacity is full, we need to remove on number at random depending on a random probability
            if(len(all_tags)>0):
                self.sequence += 1
                v = self.tagNumber - 100
                for i in range(v): 
                    self.tagNumber +=1
                    self.sampling += [all_tags[i]]
                    self.showOutput()
                for i in range(v,len(all_tags)) : 
                    probability = 100.0/self.tagNumber  # s/n
                    randomProbability = random.choices(
                        [0, 1], [probability, 1-probability])
                    if(randomProbability == [1]):
                        # remove one number randomly from the stream and add this one
                        number = random.sample(range(len(self.sampling)-1), 1)
                        removed_val = self.sampling[number[0]]
                        self.sampling.remove(self.sampling[number[0]])
                         
                        # Update the count everywhere
                        
                        self.dictionary[removed_val] -= 1
                        # add the tags
                        self.sampling += [tag[i]]
                        
                    self.showOutput()


f = open(sys.argv[2],"w")
f.write("")
f.close()
#API ACCESS TOKENS 

accessToken = "1253535121606496256-kaoTLyyLx0zJyEcjdQIFnlCCKQNF9g"
accessTokenSecret = "tpL2XgSqwVA9xwyM7YTyKC800rw87mx9aicZVXgB5dH15"
apiKey = "QdwHBSLcoBrsTRjzeQjv3dNra"
apiSecret = "1ea53OTzkB2c1oNptOBtfkC0nXBbcS92MCxPnt3ncmIPcZzYPI"
port = sys.argv[1]
#Twitter Authorization 

auth = tweepy.OAuthHandler(apiKey, apiSecret)
auth.set_access_token(accessToken, accessTokenSecret)
api = tweepy.API(auth)
streamListener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=streamListener)
# Stream 
stream.sample()


