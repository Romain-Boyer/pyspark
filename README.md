# Testing PySpark

Test de plusieurs projets avec la 
[librairie](https://spark.apache.org/docs/latest/index.html) 
PySpark.


## Projets

Une liste d'examples [ici](https://github.com/apache/spark/tree/master/examples/src/main/python).

### Estimation de Pi

Par la méthode de Monte Carlo.

### Wordcount sur 1 seul texte

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
