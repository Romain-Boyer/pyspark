import argparse
import math
import os
import shutil
import sys
from operator import add

import findspark
findspark.init()
from pyspark import SparkContext


PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))

PATH_DATA = os.path.join(PROJECT_DIR, 'data')

PATH_TEMP = os.path.join(PROJECT_DIR, 'temp')
PATH_TEMP_JOB_1 = os.path.join(PATH_TEMP, 'temp_job_1')
PATH_TEMP_RESULT = os.path.join(PATH_TEMP, 'result')


if (not os.path.isdir(PATH_DATA)) or (not os.listdir(PATH_DATA)):
    print('DL Corpus...')
    sys.exit(-1)
if os.path.isdir(PATH_TEMP):
    shutil.rmtree(PATH_TEMP)
os.makedirs(PATH_TEMP)
os.makedirs(PATH_TEMP_JOB_1)
os.makedirs(PATH_TEMP_RESULT)

CORPUS = os.listdir(PATH_DATA)
NB_DOC_IN_CORPUS = len(CORPUS)


def job_1(corpus):
    """Compute the TF for each document into the corpus

    Prend en input un dossier contenant uniquement des fichiers textes.
    Compute le wordcount, puis le nombre de mot total dans chaque texte.
    Renvoie finalement une RDD par texte avec TF = ((word, text), word_frequency)
        Avec word_frequency == wordcount / nb_words_in_doc

    input : dossier avec uniquement des textes
    output : N RDD ((word, text), TF)

    :param corpus: La liste des textes
    :return:
    """
    for text in corpus:
        text_path = os.path.join(PATH_DATA, text)
        output_path = os.path.join(PATH_TEMP_JOB_1, text + '_job_1')

        wordcount = sc \
            .textFile(text_path) \
            .flatMap(lambda x: x.translate(
                        str.maketrans('\'-', '  ', '!"#$%&()*+,./:;<=>?@[\\]^_`{|}~')).lower().split(' ')
                     ) \
            .map(lambda x: ((x, text), 1)) \
            .reduceByKey(add)

        total_words_in_doc = wordcount \
            .map(lambda x: x[1]) \
            .sum()

        wordcount \
            .map(lambda x: (x[0], x[1] / total_words_in_doc)) \
            .saveAsPickleFile(output_path)


def job_2():
    """Calcul du IDF pour tous les mots du corpus

    Retourne une RDD avec tous les mots du corpus en clef, et le IDF associé en valeur.
    L'IDF est le log( (Nombre de doc dans le corpus + 1) / (Nombre de doc qui contient le mot + 1) )

    input : N RDD ((word, text), TF)
    output : 1 RDD (word, IDF)

    :return:
    """
    idf = sc.emptyRDD()

    for temp in os.listdir(PATH_TEMP_JOB_1):
        wordfreq = sc \
            .pickleFile(os.path.join(PATH_TEMP_JOB_1, temp)) \
            .map(lambda x: (x[0][0], 1))

        idf = idf \
            .union(wordfreq) \
            .reduceByKey(add)

    return idf \
        .mapValues(lambda x: math.log((NB_DOC_IN_CORPUS + 1) / (x + 1)))


def job_3(idf, nb_to_show):
    """Calcul TF IDF pour tous mots de chaques textes du corpus

    Sauvegarde un RDD avec tous les mots de tous les textes et leurs TFIDF associé
    TF IDF = TF*IDF

    input : N RDD ((word, text), TF)
            1 RDD  (word, IDF)
    output : 1 RDD ((word, text), TFIDF)
    
    :param idf: RDD avec le idf pour tous les mots du corpus
    :param nb_to_show: can be False
    :return:
    """
    tfidf = sc.emptyRDD()

    for text in os.listdir(PATH_TEMP_JOB_1):
        text_path = os.path.join(PATH_TEMP_JOB_1, text)
        tfidf_ = sc \
            .pickleFile(text_path) \
            .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
            .join(idf) \
            .map(lambda x: ((x[0], x[1][0][0]), x[1][0][1] * x[1][1]))

        tfidf = tfidf.union(tfidf_)

    if nb_to_show:
        to_show = tfidf \
                .takeOrdered(nb_to_show, key=lambda tup: -tup[1])

        tfidf.saveAsPickleFile(os.path.join(PATH_TEMP_RESULT, 'result'))
        return to_show
    tfidf.saveAsPickleFile(os.path.join(PATH_TEMP_RESULT, 'result'))
    return False


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='TF-IDF')
    parser.add_argument(
        '--display',
        help='Nombre de mots à afficher',
        default=5,
        type=int,
    )
    args = parser.parse_args()

    sc = SparkContext(appName="TF-IDF App")
    print('\nSpark Session built !\n')

    job_1(CORPUS)
    print(f'Job 1 - Done ! TF computed and RDD saved in {PATH_TEMP_JOB_1}\n')

    idf = job_2()
    print(f'Job 2 - Done ! IDF Computed \n')

    to_show = job_3(idf, nb_to_show=args.display)
    print(f'Job 3 - Done ! TF-IDF computed and RDD saved in {PATH_TEMP_RESULT}\n')

    sc.stop()
    print('Spark Session closed!\n')

    if to_show:
        print('- '*30)
        print('Words with highest TF-IDF\n')
        for w in to_show:
            print(f"{w[0][0]} in {w[0][1]} is {w[1]}")
