# TF-IDF

> Description de tfidf.py

## Description des jobs

### job 1 - Word Count in doc

#### Input

```
N documents
```

#### Output

```
N listes de tuples : ((word, document), wordcount)
```

#### A faire

Regrouper les N listes en quelque chose...

### Job 2 - Word frequency for docs - TF

#### Input

```
N listes de tuples : ((word, document), wordcount)
```

#### Output

```
N listes de tuples : ((word, document), frequence_mot)
Avec frequence_mot = wordcount / nb_total_mots_dans_le_doc
```

#### A faire

Regrouper, et ne pas charger les données en tant que liste...

### Job 3 - Compute IDF

#### Input

```
N listes de tuples : ((word, document), frequence_mot)
```

#### Output

```
1 liste de tuples : (word, IDF)
Avec IDF = log((NB_DOC_CORPUS + 1)/(nb_doc_qui_contient_word + 1))
```

#### A faire

Supprimer cette étape, et l'intégrer dans un autre job

### Job 4 - Compute TF-IDF

#### Input

```
N listes de tuples : ((word, document), frequence_mot)
1 liste de tuples : (word, IDF)
```

#### Output

```
N listes de tuples : ((word, document), TF-IDF)
Avec TF-IDF = TF * IDF
```

#### A faire

Changer la méthode de chargement de données

## A faire

Reprendre les a faire du dessus. Mais globalement, 
* charger les données en broadcast
* mieux instancier les RDD
