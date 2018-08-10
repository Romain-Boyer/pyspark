import os
import time

import findspark

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, desc


# TODO: Refaire le pagerank avec uniquement des dataframe
# Les dataframe vont accélerer les process
# Utiliser des .parquet pour stocker
# https://spark.apache.org/docs/latest/sql-programming-guide.html


PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))

PATH_DATA = os.path.join(PROJECT_DIR, 'data')
PATH_NETWORK = os.path.join(PATH_DATA, 'epinions.txt')


spark = SparkSession.builder \
    .master('local') \
    .appName('PageRank') \
    .getOrCreate()


"""
http://infolab.stanford.edu/~ullman/mmds/ch5.pdf
page 11/38
Dans notre cas, la matrice de transition est definie :
    source == colonnes
    destination == lignes
"""
t1 = time.time()
beta = 0.8

# Crée le schema du dataframe
schema = StructType([
    StructField("source", StringType(), True),
    StructField("destination", StringType(), True),
])

# Charge le dataframe a partir du .txt
transition_matrix = spark.read.load(
    PATH_NETWORK,
    header=None,
    schema=schema,
    format='csv',
    sep='\t',
)

# Compte le nombre de source
transition_matrix_count = transition_matrix \
    .withColumnRenamed('source', 'source_') \
    .groupBy('source_') \
    .agg({'*': 'count'})

# Join les 2 matrices
transition_matrix = transition_matrix \
    .join(
        transition_matrix_count,
        transition_matrix['source'] == transition_matrix_count['source_']
    ) \
    .drop('source_') \
    .withColumnRenamed('count(1)', 'valeur')

# Inverse le score
transition_matrix = transition_matrix \
    .withColumn('valeur', beta / transition_matrix.valeur) \
    .persist()

# Initialise le vecteur pagerank
initial_pr_source = transition_matrix \
    .select('source') \
    .withColumnRenamed('source', 'pr') \
    .distinct()
initial_pr_desti = transition_matrix \
    .select('destination') \
    .withColumnRenamed('destination', 'pr') \
    .distinct()

pagerank = initial_pr_source.union(initial_pr_desti).distinct()
N = pagerank.count()
N_ = 1/N

print(f'Il y a {N} utilisateurs sur ce graph.\n')

pagerank = pagerank \
    .withColumn('score', lit(N_)) \
    .persist()

teleporting_vector = pagerank \
    .withColumn('score', (1 - beta) * pagerank.score) \
    .persist()


# DEBUT DE LA BOUCLE
for n in range(8):
    pagerank = transition_matrix \
        .join(pagerank, transition_matrix.source == pagerank.pr) \
        .drop('source') \
        .drop('pr')

    pagerank = pagerank \
        .withColumn('S', pagerank.valeur * pagerank.score) \
        .groupBy('destination') \
        .agg({'S': 'sum'}) \
        .union(teleporting_vector) \
        .groupBy('destination') \
        .agg({'sum(S)': 'sum'})

    pagerank = pagerank \
        .withColumnRenamed('destination', 'pr') \
        .withColumnRenamed('sum(sum(S))', 'score')

result = pagerank.orderBy(desc('score')).take(5)
for l, s in result:
    print(f'* ID : {l} - {s}')

print(f'\nDone in {int(time.time() - t1)} seconds.')
input('OVER ?')
spark.stop()
