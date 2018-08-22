# Testing PySpark

Test de plusieurs projets avec la 
[librairie](https://spark.apache.org/docs/latest/index.html) 
PySpark.


## Projets

Une liste d'examples [ici](https://github.com/apache/spark/tree/master/examples/src/main/python).

### Estimation de Pi

Objectif : lancer un calcul en parallèle. Vérifier que PySpark est bien installé.
Par la méthode de Monte Carlo.

### Wordcount sur 1 seul texte

Objectif : Lecture d'un texte, et calcul du wordcount

Wordcount sur le texte Anna Karenina de Tolstoy disponible 
[ici](http://www.textfiles.com/etext/FICTION/anna_karenina), et affichage des 10 mots les plus utilisés.

Pour telecharger le texte:
```bash
$ cd wordcount_1_text/data/
$ wget http://www.textfiles.com/etext/FICTION/anna_karenina
```

Pour lancer l'app, se placer dans `pyspark/` :
```bash
$ python wordcount_1_text/wordcount.py
```

### TF-IDF

> Calcul du Term Frequency - Inverse Document Frequency

Objectifs : Lecture d'un corpus de textes, et plus opérations sur ces textes.

Télécharger le corpus
```bash
$ cd tf-idf/
$ mkdir data
$ cd data
$ wget http://www.textfiles.com/etext/FICTION/defoe-robinson-103.txt
$ wget http://www.textfiles.com/etext/FICTION/callwildwget http://www.textfiles.com/etext/FICTION/dracula
$ wget http://www.textfiles.com/etext/FICTION/gulistan
$ wget http://www.textfiles.com/etext/FICTION/tess10.txt
$ wget http://www.textfiles.com/etext/FICTION/nabby10.txt
```

Pour lancer le programme, une fois un corpus télécharger et mis dans `tf-idf/data/`, 
se placer dans la racine du projet et lancer :
```bash
$ python tf-idf/tfidf_V2.py --display N
```
Avec `N` le nombre de mots à afficher dans la console.
Si N n'est pas renseigné, il vaut automatiquement 10.

### Trees of Paris

Objectif : s'entrainer avec du csv dans pyspark.

Les données sont la : [opendata](https://opendata.paris.fr/explore/dataset/les-arbres/table/). 

Calcul de la taille et frequence des arbres de Paris...

```bash
$ python tree_of_paris/trees.py
```

### PageRank

Calculate the PageRank score (with damping factor 0.85) for each user in the 
Epinions who-trust-whom online social network : 
`https://snap.stanford.edu/data/soc-Epinions1.html`.

```bash
$ python pagerank/pagerank.py
```

### PageRank

Calculate the PageRank score (with damping factor 0.85) for each user in the 
Epinions who-trust-whom online social network : 
`https://snap.stanford.edu/data/soc-Epinions1.html`.

```bash
$ python pagerank/pagerank.py
```

## Installations
### Prérequis

Installer [Spark](https://spark.apache.org/docs/latest/) !
Puis, dans un virtual environnement, installer les librairies :

```bash
$ pip install -r requirements.txt
```

### Commandes utiles

Pour vérifier le nombre de coeurs qui sont utilisés : 
````bash
$ htop
````
(un `brew install htop` peut être nécessaire).

Pour installer graphframes :
````bash
$ pip install "git+https://github.com/munro/graphframes.git@release-0.5.0#egg=graphframes&subdirectory=python"
````

Pour utiliser graphframes sur un jupyter notebook. Pas très propre, mais ça marche ...
```bash
$ mkdir ~/jupyter
$ cd ~/jupyter
$ wget https://github.com/graphframes/graphframes/archive/release-0.2.0.zip
$ unzip release-0.2.0.zip
$ cd graphframes-release-0.2.0
$ build/sbt assembly
$ cd ..

# Copy necessary files to root level so we can start pyspark. 
$ cp graphframes-release-0.2.0/target/scala-2.11/graphframes-release-0-2-0-assembly-0.2.0-spark2.0.jar .
$ cp -r graphframes-release-0.2.0/python/graphframes .

# Set environment to use Jupyter
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS=notebook

# Launch the jupyter server.
$ pyspark --jars graphframes-release-0-2-0-assembly-0.2.0-spark2.0.jar
```

Autre lien qui a l'air bien : `https://www.datareply.co.uk/blog/2016/9/20/running-graph-analytics-with-spark-graphframes-a-simple-example`

### Liens sympas

1. RDD ou DF
[Databricks](https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html)
[Video](https://databricks.com/session/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets)
