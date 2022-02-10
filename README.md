# YoutubeTrends

## Overview

Dans le cadre d'un projet sur l'utilisation d'Apache SPARK,  nous devions choisir un jeu de données et répondre à une problématique lié à ce dataset à l'aide d'Apache SPARK.
Nous avons choisi un dataset sur les Vidéos Youtube "Tendances" disponible sur Kaggle. Pour plus de détail voir [lien](https://www.kaggle.com/datasnaek/youtube-new?select=FRvideos.csv "Dataset Kaggle")

Notre problématique était simple : **Interpréter les tendances Youtube et essayer de découvrir des méchanismes, facteurs de tendance**

### Dataset

Le dataset se divise en deux partie;

Premièrement en un fichier json contenant des informations sur les categories et notamment le nom d'une catégorie pour un id donné (les deux informations qui nous importent ici)

Deuxièmement en fichiers csv contenant toutes les informations nécéssaires sur les vidéos en tendances. Chaque pays a un fichier qui lui est propre (USA, Great Britain, Germany, Canada, France, Russia, Mexico, South Korea, Japan, India)

Un exemple de ligne :

| video_id    | channel_title     | trending_date | title                                               | category_id | publish_time                | tags                                                                                                          | views     | likes   | dislikes | comment_count | thumbnail_link                                 | comments_disabled | ratings_disabled | video_error_or_removed | description                                                                                             |
|-------------|-------------------|---------------|-----------------------------------------------------|-------------|-----------------------------|---------------------------------------------------------------------------------------------------------------|-----------|---------|----------|---------------|------------------------------------------------|-------------------|------------------|------------------------|---------------------------------------------------------------------------------------------------------|
| FlsCjmMhFmw | YouTube Spotlight | 17.10.12      | YouTube Rewind: The Shape of 2017 \| #YouTubeRewind | 24          | 24 2017-12-06T17:58:51.000Z | Rewind"\|"Rewind 2017"\|"youtube rewind 2017"\|"#YouTubeRewind"\|"Rewind 2016"\|"Dan and Phil"\|"Grace Hel... | 100911567 | 2656682 | 1353661  | 682890        | https://i.ytimg.com/vi/FlsCjmMhFmw/default.jpg | False             | False            | False                  | YouTube Rewind 2017. Celebrating the videos, people, music and memes that made 2017. #YouTubeRewind\... |

## Projet

### Traitement des données

Certaines informations ne nous étaient pas utilise dans le cadre de notre démarche, nous les avons donc pas retenues.
L'information "category_id" est difficilement inteprétable, nous avons donc rajouté une nouvelle colonne "category_name" en nous basant sur le fichier json category.

Voilà à quoi ressemble nos nouvelles lignes de données : 

### Fonctionnalitées 


### Setup

### Languages - Frameworks utilisés 

* Spark
* Scala
* SQL

### Environnement 

* Le projet a été développé avec intellij IDEA
* Scala sdk 2.12.15
* Java sdk 11.0.5
