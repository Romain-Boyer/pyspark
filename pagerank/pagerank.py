import os
import re
import time
from operator import add

import findspark

findspark.init()
from pyspark.sql import SparkSession

PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))

PATH_DATA = os.path.join(PROJECT_DIR, 'data')
PATH_NETWORK = os.path.join(PATH_DATA, 'epinions.txt')

"""
Description :
v' = β*M*v + (1 - β)*(1/n)*e
avec v qui commence (1/n, 1/n, ..., 1/n)
M la matrice qui représente les liens entre les utilisateurs
b : beta, souvent entre 0.8 et 0.9

Recall β is a constant slightly less than 1, e is a vector of all 1’s, and n is the
number of nodes in the graph that transition matrix M represents.

Vu que la matrice de transition est pleine de 0, nous allons l'écrire sous une liste de tuples
de la forme [(ligne, col, valeur), ...].
Le vecteur PageRank est de la même forme [(coord, valeur), ...]

Pour multiplier ces 2 matrices,
il faut multiplier tous les termes de la colonne i de M avec ceux de coord i du vecteur.
On regroupe suivant la ligne de la matrice, on somme, on a notre nouveau vecteur.

Il y a un problème dans ce pagerank, la somme des valeurs du vecteur pagerank est différente de 1
"""


def get_transition_matrix(file_path, spark):
    """Renvoie la représentation de la matrice de transition.
    Puisque qu'elle est composée de nombreux 0, on la représente sous la forme d'une liste de tuples =
    [(ligne, colonne, valeur), ...]
    
    :param file_path: le chemin du .txt qui contient les liens et est prêt à être chargé
    :param spark: spark context
    :return: la matrice de transition
    """
    # ouvre le fichier texte, en supposant qu'il commence directement, et qu'il est : 'i\tj'
    lines = spark.read.text(file_path).rdd.map(lambda r: r[0])

    # Crée la matrice de transition M
    transition_matrix = lines.map(lambda user: _parse_users(user) + (1,)).persist()

    # Compte le nombre de termes non nuls par colonne
    normalisation = transition_matrix.map(lambda row: (row[0], 1)).reduceByKey(add)

    transition_matrix_normal = transition_matrix \
        .leftOuterJoin(normalisation) \
        .map(lambda x: (x[1][0], x[0], 1 / x[1][1]))

    return transition_matrix_normal


def get_initial_vector(M):
    """On cherche à avoir tous les éléments présents dans la matrice de transition
    
    :param M: matrice de transition
    :return: vecteur, nombre d'élements
    """
    v = M.flatMap(lambda x: (x[0], x[1])).distinct()
    n_ = v.count()

    return v.map(lambda x: (x, 1 / n)), n_


def iteration(transition_matrix, pagerank_vector, n, beta):
    """Réalise une iteration v' = β*M*v + (1 - β)*(1/n)*e

    :return: pagerank_vector
    """
    new_pagerank_vector = transition_matrix \
        .map(lambda x: (x[1], (x[0], x[2]))) \
        .leftOuterJoin(pagerank_vector) \
        .map(lambda x: (x[1][0][0], x[1][0][1] * (x[1][1] or 0))) \
        .reduceByKey(add) \
        .rightOuterJoin(pagerank_vector) \
        .mapValues(lambda x: (x[0] or 0) * beta + (1 - beta) / n)

    return new_pagerank_vector


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
    spark = SparkSession.builder \
        .master('local') \
        .appName('PageRank') \
        .getOrCreate()
    print(f'\nSpark Session built !\n')

    transition_matrix = get_transition_matrix(PATH_NETWORK, spark).persist()
    print(f'Transition matrix built.\n')

    pagerank_vector, n = get_initial_vector(transition_matrix)

    print(f'Page rank initialized !')
    print(f"Il y a {n} utilisateurs différents présents sur ce graphe.\n")

    N = 8
    for i in range(N):
        pagerank_vector = iteration(transition_matrix, pagerank_vector, n, 0.80)

    #pagerank_vector.persist()
    print(f'{N} iter done.')

    print('\nBest resultats :')
    results = pagerank_vector.takeOrdered(5, lambda tup: -tup[1])
    for k, v in results:
        print(f"ID : {k} with score of {v}")

    # Debuggage
    print(f'\nDone in {int(time.time() - t1)} seconds.')
    input('OVER ?')

    spark.stop()
    print('\nSpark Session closed!')
