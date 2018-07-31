import os
import sys
from operator import add

import findspark
findspark.init()
from pyspark.sql import SparkSession


PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))


def wordcount(text):
    """
    sc
        SparkSession : The entry point to programming Spark with the Dataset and DataFrame API.
        .builder : A class attribute having a Builder to construct SparkSession instances.
        .getOrCreate() : Gets an existing SparkSession or, if there is no existing one,
                         creates a new one based on the options set in this builder.

    lines
        .read.text(text_path) : Loads text files and returns a DataFrame whose schema starts with
                                a string column named “value”, and followed by partitioned columns if there are any.
                                    df = sc.read.text(text_path)
                                    df.collect()
        .rdd : A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
        .map(lambda x: ) : Return a new RDD by applying a function to each element of this RDD.

    counts
        .flatMap(lambda x: ) : Return a new RDD by first applying a function to all elements of this RDD,
                               and then flattening the results.
        .reduceByKey( ) : Merge the values for each key using an associative and commutative reduce function.
    """
    sc = SparkSession.builder.appName("WordCount").getOrCreate()

    lines = sc.read.text(text).rdd.map(lambda r: r[0])

    counts = lines\
        .flatMap(lambda x: x.translate(str.maketrans('', '', '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')).split(' '))\
        .map(lambda x: (x, 1))\
        .reduceByKey(add)

    output_ = counts.collect()

    sc.stop()

    return sorted(output_, key=lambda tup: -tup[1])


def check_data():
    if os.path.isfile(os.path.join(PROJECT_DIR, 'data', 'anna_karenina')):
        return os.path.join(PROJECT_DIR, 'data', 'anna_karenina')
    print('Text not found !')
    sys.exit(-1)


if __name__ == '__main__':

    text = check_data()
    output = wordcount(text)

    print(f"Most used words :")
    for i in range(1, 10):
        print(f"    {output[i][0]} {output[i][1]} times.")
