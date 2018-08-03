import os
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")

import findspark
findspark.init()
from pyspark.sql import SparkSession

PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))

PATH_DATA = os.path.join(PROJECT_DIR, 'data')


"""
# Le fichier arbres.csv est propre. Aucune valeur manquante. Le preprocessing suivant a été le suivant :

df = pd.read_csv('/Users/rboyer/Downloads/les-arbres.csv', sep=";")

df_ = df[['IDBASE', 'GENRE', 'ESPECE', 'HAUTEUR (m)', 'CIRCONFERENCEENCM', 'ARRONDISSEMENT', 'STADEDEVELOPPEMENT']]
df_.columns = ['idbase', 'genre', 'espece', 'hauteur', 'circonf', 'arrondissement', 'stade_de_developpement']
df_= df_.fillna({
    'stade_de_developpement':'NR', 
    'espece':'NR',
    'genre':'NR'
})

df_.to_csv('/Users/rboyer/dev/pyspark/tree_of_paris/data/arbres.csv', index=False)
"""


def tree_by_gender(arbres, to_display=5):
    """Calcul le nombre d'arbres par gender

    :param arbres: pyspark dataframe
    :param to_display: int
    :return:
    """
    nb_tree_by_type = arbres \
        .groupBy('genre') \
        .count() \
        .orderBy('count', ascending=False) \
        .limit(to_display) \
        .toPandas()

    return nb_tree_by_type


def highest_by_gender(arbres, to_display=5):
    """On garde le plus grand des arbres par espece. Puis on ordonne par cela

    :param arbres:
    :param to_display:
    :return:
    """
    height_by_gender = arbres \
        .groupBy('genre') \
        .agg({'hauteur': 'max'}) \
        .orderBy('max(hauteur)', ascending=False) \
        .limit(to_display) \
        .toPandas()

    return height_by_gender


def borough_fatest(arbres):

    borough_oldest = arbres \
        .groupBy('arrondissement') \
        .avg('circonf') \
        .orderBy('avg(circonf)') \
        .withColumnRenamed('avg(circonf)', 'avg_circonf') \
        .first()

    return borough_oldest


if __name__ == '__main__':

    # Instancie Spark Session
    spark = SparkSession.builder \
        .master('local') \
        .appName('Trees of Panam') \
        .getOrCreate()
    print(f'\nSpark Session built !\n')


    arbres = spark.read.load(
        os.path.join(PATH_DATA, 'arbres.csv'),
        format='csv',
        sep=',',
        inferSchema='true',
        header='true',
    )
    print('DataFrame loaded !\n')


    # Affiche les N arbres les plus représentés à Paris
    N = 5
    most_common_trees = tree_by_gender(arbres, N)
    print('- '*30)
    print(f'I - The {N} most common trees in Paris\n')
    print(most_common_trees)
    print('- ' * 30)


    # Affiche les 5 especes avec les arbres les plus grands.
    highest_gender = highest_by_gender(arbres, N)
    print(f'II - Les {N} plus grandes "especes" à Paris\n')
    print(highest_gender)
    print('- ' * 30)


    # Affiche le quartier avec les arbres les plus vieux en moyenne
    oldest = borough_fatest(arbres)
    print(f"III - L'arrondissement avec les plus vieux arbres est {oldest.arrondissement} "
          f"avec une hauteur moyenne de {int(oldest.avg_circonf)} m.")
    print('- ' * 30 + '\n')


    spark.stop()
    print('Spark Session closed !')
