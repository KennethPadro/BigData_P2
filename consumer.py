#!/usr/bin/env python
# -*- coding: utf-8 -*-

import findspark
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#findspark.init()

spark = SparkSession.builder.getOrCreate()


df = spark.read.json('/home/user/kbpadro/tweets.json')


spark = SparkSession.builder.getOrCreate()

stop = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "he’d", "he’ll", "he’s", "her", "here", "here’s", "hers", "herself", "him", "himself", "his", "how", "how’s", "I", "I’d", "I’ll", "I’m", "I’ve", "if", "in", "into", "is", "it", "it’s", "its", "itself", "let’s", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she’d", "she’ll", "she’s", "should", "so", "some", "such", "than", "that", "that’s", "the", "their", "theirs", "them", "themselves", "then", "there", "there’s", "these", "they", "they’d", "they’ll", "they’re", "they’ve", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "we’d", "we’ll", "we’re", "we’ve", "were", "what", "what’s", "when", "when’s", "where", "where’s", "which", "while", "who", "who’s", "whom", "why", "why’s", "with", "would", "you", "you’d", "you’ll", "you’re", "you’ve", "your", "yours", "yourself", "yourselves"]
searchwords = ['trump', 'flu', 'zika', 'diarrhea', 'ebola', 'headache', 'measles']


while(hour<=72):
        hashtagsList = []
        keywordsStrList = []
        specificWordsStr = []
        for r in hashtags.rdd.collect():
                if r['entities']:
                        for hashtag in r['entities']['hashtags']:
                                hashtagsList.append(hashtag['text'])
                if r['extended_tweet']:
                        for keyword in r['extended_tweet']['full_text'].encode('utf-8').split():
                                if keyword.lower() not in stop:
                                        keywordsStrList.append(keyword.lower())
                                for word in searchwords:
                                        if word in keyword.lower():
                                                specificWordsStr.append(word)


        df = spark.read.json('/home/user/kbpadro/tweets.json')
        df.createOrReplaceTempView("hashtags")
        sqlResult = spark.sql("select Hashtag, count(*) as Total from hashtags group by Hashtag order by Total desc limit 10")
        jsonResult = sqlResult.toJSON().collect()
        print(jsonResult)

        df = spark.read.json('/home/user/kbpadro/tweets.json')
        df.createOrReplaceTempView("users")
        sqlResult = spark.sql(
            "select user.id, count(*) as Total from users group by User order by Total desc limit 10")
        jsonResult = sqlResult.toJSON().collect()
        print(jsonResult)

        df = spark.read.json('/home/user/kbpadro/tweets.json')
        df.createOrReplaceTempView("words")
        sqlResult = spark.sql(
            "select Keyword, count(*) as Total from words group by Keyword order by Total desc limit 10")
        jsonResult = sqlResult.toJSON().collect()
        print(jsonResult)
