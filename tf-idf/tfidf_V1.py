import argparse
import math as m
import os
import pickle
import sys
from operator import add

import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession


PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))
PATH_DATA = os.path.join(PROJECT_DIR, 'data')
PATH_TEMP = os.path.join(PROJECT_DIR, 'temp')
PATH_TEMP_JOB_1 = os.path.join(PATH_TEMP, 'temp_job_1')
PATH_TEMP_JOB_2 = os.path.join(PATH_TEMP, 'temp_job_2')
PATH_TEMP_JOB_3 = os.path.join(PATH_TEMP, 'temp_job_3')
PATH_RESULT = os.path.join(PATH_TEMP, 'result')


if (not os.path.isdir(PATH_DATA)) or (not os.listdir(PATH_DATA)):
    print('DL Corpus...')
    sys.exit(-1)
if not os.path.isdir(PATH_TEMP):
    os.makedirs(PATH_TEMP)
if not os.path.isdir(PATH_TEMP_JOB_1):
    os.makedirs(PATH_TEMP_JOB_1)
if not os.path.isdir(PATH_TEMP_JOB_2):
    os.makedirs(PATH_TEMP_JOB_2)
if not os.path.isdir(PATH_TEMP_JOB_3):
    os.makedirs(PATH_TEMP_JOB_3)
if not os.path.isdir(PATH_RESULT):
    os.makedirs(PATH_RESULT)

CORPUS = os.listdir(PATH_DATA)
NB_DOC_IN_CORPUS = len(CORPUS)


def job_1(corpus):
    for text in corpus:
        text_path = os.path.join(PATH_DATA, text)

        sc = SparkSession.builder.appName("Job 1 - Word Count in Doc").getOrCreate()
        lines = sc.read.text(text_path).rdd.map(lambda row: row[0])
        counts = lines\
                .flatMap(lambda x: x.translate(str.maketrans('\'-', '  ', '!"#$%&()*+,./:;<=>?@[\\]^_`{|}~')).split(' '))\
                .map(lambda x: ((x, text), 1))\
                .reduceByKey(add)

        output_ = counts.collect()
        output = sorted(output_, key=lambda tup: -tup[1])

        sc.stop()

        with open(os.path.join(PATH_TEMP_JOB_1, text), 'wb') as fp:
            pickle.dump(output, fp)


def job_2():
    for wordcount_ in CORPUS:

        with open(os.path.join(PATH_TEMP_JOB_1, wordcount_), 'rb') as fp:
            wordcount = pickle.load(fp)

        sc = pyspark.SparkContext(appName="Job 2 - Word Frequency for Docs - TF")
        total_words_in_doc = sc.parallelize(wordcount).map(lambda x: x[1]).sum()

        output = sc.parallelize(wordcount).map(lambda x: (x[0], x[1] / total_words_in_doc)).collect()

        sc.stop()

        with open(os.path.join(PATH_TEMP_JOB_2, wordcount_), 'wb') as fp:
            pickle.dump(output, fp)


def job_3():
    all_words = []
    for wordcount_ in CORPUS:
        with open(os.path.join(PATH_TEMP_JOB_2, wordcount_), 'rb') as fp:
            all_words += pickle.load(fp)

    sc = pyspark.SparkContext(appName="Job 3 - Compute IDF")

    mapping = sc.parallelize(all_words).map(lambda x: (x[0][0], (x[0][1], x[1])))

    counts = mapping.groupByKey().mapValues(len).mapValues(lambda x: m.log((NB_DOC_IN_CORPUS + 1) / (x + 1))).collect()

    sc.stop()

    with open(os.path.join(PATH_TEMP_JOB_3, 'idf'), 'wb') as fp:
        pickle.dump(counts, fp)


def job_4():
    with open(os.path.join(PATH_TEMP_JOB_3, 'idf'), 'rb') as fp:
        idf = pickle.load(fp)

    for doc in CORPUS:
        sc = pyspark.SparkContext(appName="Last Job - Compute TF-IDF")

        with open(os.path.join(PATH_TEMP_JOB_2, doc), 'rb') as fp:
            tf = pickle.load(fp)

        tf = sc.parallelize(tf).map(lambda x: (x[0][0], (x[0][1], x[1])))
        idf_ = sc.parallelize(idf)

        tfidf = tf.join(idf_).map(lambda x: ((x[0], x[1][0][0]), x[1][0][1] * x[1][1]))\
            .sortBy(lambda x: -x[1]).collect()

        sc.stop()

        with open(os.path.join(PATH_RESULT, doc), 'wb') as fp:
            pickle.dump(tfidf, fp)


def display_results(display=5):
    if not display > 0:
        return

    all_tfidf = []
    for doc in CORPUS:
        with open(os.path.join(PATH_RESULT, doc), 'rb') as fp:
            all_tfidf += pickle.load(fp)

    sc = pyspark.SparkContext(appName="Sort by score")

    all_tfidf = sc.parallelize(all_tfidf).sortBy(lambda tup: -tup[1]).collect()

    sc.stop()

    print('\nResults :')
    for i in range(display):
        print(f"TF-IDF of {all_tfidf[i][0][0]} in {all_tfidf[i][0][1]} is {all_tfidf[i][1]}")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='TF-IDF')
    parser.add_argument(
        '--display',
        help='Nombre de mots à afficher',
        default=5,
        type=int,
    )
    args = parser.parse_args()

    job_1(CORPUS)
    print('Job 1 done'+' -'*30)

    job_2()
    print('Job 2 done'+' -'*30)

    job_3()
    print('Job 3 done'+' -'*30)

    job_4()
    print('TFIDF Computed'+' -'*25)

    display_results(args.display)
