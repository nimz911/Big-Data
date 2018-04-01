# -*- coding: utf-8 -*-
"""
Created on Sun Apr  1 14:38:47 2018

@author: nimrod
"""
#%%
######  Start SparkContext
from pyspark import SparkContext, SparkConf
sc = SparkContext.getOrCreate()


#%%

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


# Create new config
conf = (SparkConf()
    .set("spark.driver.maxResultSize", "0"))\
    .set("spark.executor.cores","300") \
    .setMaster('local[30]') \
    .set("spark.executor.memory", "200g") \
    .set("spark.driver.memory", "200g") \
    .set("spark.python.worker.memory","200g") \
    .set("spark.local.dir","C:\sparkTMP") \
    .set("spark.default.parallelism","30") \
    .set("spark.python.worker.memory","200g") \
    .set("spark.app.name","pyspark_reddit") \
    .set("spark.driver.extraJavaOptions","-Xmx204800m") \
# Create new context
sc = SparkContext(conf=conf) 
sqlContext = SQLContext(sc)

#%%

from pyspark.sql.types import * 
from pyspark.sql.functions import *
import pandas as pd

reddit = sqlContext.read.load("C:/reddit_rd/*", format="json")
#%%
data = reddit.select(reddit['author'], reddit['subreddit'], reddit['created_utc'])
data.printSchema()
#%%
#'MMM d, yyyy h:mm:ss aa'
from_pattern = 'ssssssssss'
to_pattern = 'yyyy'
reddit_year = reddit.withColumn('year', from_unixtime(unix_timestamp(reddit['created_utc'], from_pattern), to_pattern))
#%%
subreddit_year = reddit_year.groupBy("subreddit","year").count().orderBy(desc("count")).toPandas()
#%%
subreddit = reddit.groupBy("subreddit").count().orderBy(desc("count")).toPandas()
#%%
top20 = subreddit_year.loc[subreddit_year['subreddit'].isin(subreddit.head(20)['subreddit'])]
top20['rank'] = top20['subreddit'].rank()
top20 = top20.sort_values(['subreddit','year'])
#%%
%matplotlib inline
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm
from matplotlib.pyplot import *
from matplotlib.ticker import FormatStrFormatter

x = top20['year'].astype(float)
y = top20['count']


xlabels = x
ylabels = y
#matplotlib.rcParams.update({'font.size': 20})
s = 250
c = top20['rank']
    # plot size
plt.rcParams["figure.figsize"] = (30,20)
    # x/y ticks
plt.scatter(x, y, alpha=0.7, marker="h",c=c)
plt.xticks(x, xlabels, rotation=30,size=25, color='greenyellow')
plt.yticks(y, ylabels,color='greenyellow',size=20)
plt.yscale("log", nonposy='clip')
    # title
#plt.title('SUBREDDITS - TOP 20', fontsize=70, color='greenyellow')
    # x/y labels
#plt.xlabel('subreddit', fontsize=50, color='greenyellow')
#plt.ylabel('data size in GB', fontsize=50, color='greenyellow')

#plt.ylim((10012353,320704526))
#plt.xlim((152,180))
plt.margins(0.01)
plt.subplots_adjust(bottom=0.008)
    #plot backgroup
plt.rcParams['axes.facecolor'] = 'gainsboro'
#plt.gca().yaxis.grid(True,linewidth=0.8, color='grey',linestyle='-.')
#plt.gca().xaxis.grid(True,linewidth=0.8, color='grey',linestyle='-.')
#plt.gca().yaxis.set_minor_formatter(FormatStrFormatter("%.2f"))

plt.show()
#%%
import plotly
p = plotly.tools.set_credentials_file(username='', api_key='')

#%%
import plotly.plotly as py
import plotly.graph_objs as go

trace = go.Heatmap(x = top20['year'].astype(float),
                   y = top20['subreddit'],
                   z = top20['count'],
                   colorscale=[[0, 'rgb(166,206,227)'], [0.25, 'rgb(31,120,180)'], [0.45, 'rgb(178,223,138)'], [0.65, 'rgb(51,160,44)'], [0.85, 'rgb(251,154,153)'], [1, 'rgb(227,26,28)']])
data=[trace]

layout = go.Layout(
    title='reddit commits per year',
    xaxis = dict(ticks='', nticks=36),
    yaxis = dict(ticks='' )
)

#
#py.iplot(data, filename='simple-colorscales-colorscale', layout=layout)
py.iplot(data, filename='colorscales-custom-colorscale', layout=layout)


#%%