import os
import re
import time
from operator import add

import findspark

findspark.init()
from pyspark.sql import SparkSession

# TODO: Arriver à diminuer le temps de computation après 5 iterations
# Je ne sais pas pourquoi cela marche comme ca et ca me gonfle !
# Il faudrait arriver à sauvegarder le dernier pagerank_vector
# puis le charger, blablabla


PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))

PATH_DATA = os.path.join(PROJECT_DIR, 'data')
PATH_NETWORK = os.path.join(PATH_DATA, 'epinions.txt')


def _parse_users(row):
    """On sépare une ligne qui représente "site 1" pointe vers "site 2" de la forme "site1\tsite2"
    On sépare un string avec une tabulation en 2

    :param row: la ligne du fichier texte
    :return:
    """
    parts = re.split(r'\t', row)
    return parts[0], parts[1]


if __name__ == '__main__':

    t1 = time.time()
    beta = 0.8

    spark = SparkSession.builder \
        .master('local') \
        .appName('PageRank') \
        .getOrCreate()
    print(f'\nSpark Session built !\n')

    # ouvre le fichier texte, en supposant qu'il commence directement, et qu'il est : 'i\tj'
    lines = spark.read.text(PATH_NETWORK).rdd.map(lambda r: r[0])

    # Crée la matrice de transition M
    transition_matrix_ = lines.map(lambda user: _parse_users(user) + (1,)) \
        .persist()

    # Compte le nombre de termes non nuls par colonne
    normalisation = transition_matrix_.map(lambda row: (row[0], 1)) \
        .reduceByKey(add)

    # Normalise la matrice de transition
    transition_matrix = transition_matrix_ \
        .leftOuterJoin(normalisation) \
        .map(lambda x: (x[1][0], x[0], 1 / x[1][1])) \
        .persist()

    transition_matrix_.unpersist()

    # Initialise le pagerank vecteur avec tous les éléments du graph
    pagerank_vector = transition_matrix \
        .flatMap(lambda x: (x[0], x[1])) \
        .distinct()

    # Compte le nombre d'éléments uniques dans le graph
    n = pagerank_vector.count()

    # Initialise le pagerank vecteur
    pagerank_vector = pagerank_vector \
        .map(lambda x: (x, 1 / n)) \
        .persist()

    N = 8
    for i in range(N):
        # Calcul de la premiere iter
        new_pagerank_vector = transition_matrix \
            .map(lambda x: (x[1], (x[0], x[2]))) \
            .leftOuterJoin(pagerank_vector) \
            .map(lambda x: (x[1][0][0], x[1][0][1] * (x[1][1] or 0))) \
            .reduceByKey(add) \
            .rightOuterJoin(pagerank_vector) \
            .mapValues(lambda x: (x[0] or 0) * beta + (1 - beta) / n)

        pagerank_vector.unpersist()

        pagerank_vector = new_pagerank_vector.persist()

    results = pagerank_vector.takeOrdered(5, lambda tup: -tup[1])
    for k, v in results:
        print(f"ID : {k} with score of {v}")

    print(f'\nDone in {int(time.time() - t1)} seconds.')
    input('OVER ?')
    spark.stop()
