import os
import re
import time

import findspark

findspark.init()
import pyspark

PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))

PATH_DATA = os.path.join(PROJECT_DIR, 'data')
PATH_NETWORK = os.path.join(PATH_DATA, 'epinions.txt')

t1 = time.time()

beta = 0.8


def parse_users(row):
    parts = re.split(r'\t', row)
    return parts[0], parts[1]


def rankContribution(uris, rank, beta, n):
    numberOfUris = len(uris)
    no_rank = (1 - beta) / n
    rankContribution = float(rank or no_rank) / numberOfUris
    newrank = []
    for uri in uris:
        newrank.append((uri, rankContribution))
    return newrank


sc = pyspark.SparkContext(appName="PR")

# 1 - Load the .txt
page_links_rdd = sc.textFile(PATH_NETWORK).map(lambda r: parse_users(r))

# 2 - Create page_ranks_rdd
page_ranks_rdd = page_links_rdd.flatMap(lambda x: (x[0], x[1])).distinct()

n = page_ranks_rdd.count()

page_ranks_rdd = page_ranks_rdd.map(lambda x: (x, 1 / n)).partitionBy(2, hash).persist()

print(f"{n} users !")

# 3 - Persist page_links_rdd
page_links_rdd = page_links_rdd.groupByKey().mapValues(list).partitionBy(2, hash).persist()

# 4 - Loop
for i in range(8):
    linksRank = page_links_rdd.leftOuterJoin(page_ranks_rdd)
    contributedRDD = linksRank.flatMap(lambda x: rankContribution(x[1][0], x[1][1], beta, n))
    sumRanks = contributedRDD.reduceByKey(lambda v1, v2: v1 + v2)
    page_ranks_rdd = sumRanks.map(lambda x: (x[0], (1 - beta) / n + beta * x[1]))

# 5 - Display Results
result = page_ranks_rdd.takeOrdered(5, key=lambda x: -x[1])

for l, s in result:
    print(f'* ID : {l} - {s}')

print(f'\nDone in {int(time.time() - t1)} seconds.')
input('OVER ?')
sc.stop()
