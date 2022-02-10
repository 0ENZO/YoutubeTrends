# YoutubeTrends

# Overview

Dans le cadre d'un projet sur l'utilisation d'Apache SPARK,  nous devions choisir un jeu de données et répondre à une problématique lié à ce dataset à l'aide d'Apache SPARK.
Nous avons choisi un dataset sur les Vidéos Youtube "Tendances" disponible sur Kaggle. Pour plus de détail voir [lien](https://www.kaggle.com/datasnaek/youtube-new?select=FRvideos.csv "Dataset Kaggle")

Notre problématique était simple : **Interpréter les tendances Youtube et essayer de découvrir des méchanismes, facteurs de tendance**

## Dataset

Le dataset se divise en deux partie;

Premièrement en un fichier json contenant des informations sur les categories et notamment le nom d'une catégorie pour un id donné (les deux informations qui nous importent ici)

Deuxièmement en fichiers csv contenant toutes les informations nécéssaires sur les vidéos en tendances. Chaque pays a un fichier qui lui est propre (USA, Great Britain, Germany, Canada, France, Russia, Mexico, South Korea, Japan, India)

Un exemple de ligne :

| video_id    | channel_title     | trending_date | title                                               | category_id | publish_time                | tags                                                                                                          | views     | likes   | dislikes | comment_count | thumbnail_link                                 | comments_disabled | ratings_disabled | video_error_or_removed | description                                                                                             |
|-------------|-------------------|---------------|-----------------------------------------------------|-------------|-----------------------------|---------------------------------------------------------------------------------------------------------------|-----------|---------|----------|---------------|------------------------------------------------|-------------------|------------------|------------------------|---------------------------------------------------------------------------------------------------------|
| FlsCjmMhFmw | YouTube Spotlight | 17.10.12      | YouTube Rewind: The Shape of 2017 \| #YouTubeRewind | 24          | 24 2017-12-06T17:58:51.000Z | Rewind"\|"Rewind 2017"\|"youtube rewind 2017"\|"#YouTubeRewind"\|"Rewind 2016"\|"Dan and Phil"\|"Grace Hel... | 100911567 | 2656682 | 1353661  | 682890        | https://i.ytimg.com/vi/FlsCjmMhFmw/default.jpg | False             | False            | False                  | YouTube Rewind 2017. Celebrating the videos, people, music and memes that made 2017. #YouTubeRewind\... |

# Projet

## Traitement des données

Certaines informations ("video_id", "thumbnail_link", "tags") ne nous étaient pas utile dans le cadre de notre démarche, nous les avons donc pas retenues. 

L'information "category_id" étant difficilement inteprétable, nous avons donc rajouté une nouvelle colonne "category_name" en nous basant sur le fichier json category.

Voilà à quoi ressemble nos nouvelles lignes de données : 

| channel_title     | trending_date | title                                               | category_id | publish_time                | views     | likes   | dislikes | comment_count | comments_disabled | ratings_disabled | video_error_or_removed | description                                                                                             | category_name |
|-------------------|---------------|-----------------------------------------------------|-------------|-----------------------------|-----------|---------|----------|---------------|-------------------|------------------|------------------------|---------------------------------------------------------------------------------------------------------|---------------|
| YouTube Spotlight | 17.10.12      | YouTube Rewind: The Shape of 2017 \| #YouTubeRewind | 24          | 24 2017-12-06T17:58:51.000Z | 100911567 | 2656682 | 1353661  | 682890        | False             | False            | False                  | YouTube Rewind 2017. Celebrating the videos, people, music and memes that made 2017. #YouTubeRewind\... | Entertainment |

## Fonctionnalitées 

Les divers exemples ci-dessous se basent sur le dataset français.

`readVideosFile(spark, "FR")` &rarr; Récupère un fichier selon le code langue et le renvoie dans un dataframe


`readCategoriesFile(spark)` &rarr; Récupère le fichier category et le renvoie dans un dataframe


`getGlobalVideosDf(spark, lang_code_to_continent)` &rarr; Créé un dataframe regroupant tous les fichiers du dataset hormis category, rajoute également deux nouvelles colonnes "lang_code" ainsi que "continent" ("North_America", "Europe", "Asia", "Russia")  


`getTotalViewsPerCategoryForSpecificChannel(spark, df, artist)` &rarr; Renvoie un dataframe avec, pour une chaîne youtube donnée, le nombre de vidéos faites par catégorie ainsi que le nombre total de vues associé

Exemple avec la chaîne youtube "Anil B" : 

<img width="300" alt="getTotalViewsPerCategoryForSpecificChannel" src="https://user-images.githubusercontent.com/53021621/153435398-a86940f2-d10d-47bc-8ae7-52d7d83b14c6.PNG">


`getMostWatchedChannelsForSpecificYear(df, yearRequested)` &rarr; -> Renvoie un dataframe avec les chaînes youtube ayant été le plus visionné pour des vidéos parues une année spécifique :warning: A ne pas mal interpréter ces résultats, le dataset "FR" ayant été sélectionné cela ne veut pas dire que l'audience ou que la chaîne sont exclusivement françaises. De même, l'année concerne l'année de parution de la vidéo et non le nombre de vues réalisées pour telle année. 

Exemple :

<img width="300" alt="getMostWatchedChannelsForSpecificYear" src="https://user-images.githubusercontent.com/53021621/153436591-f53fb7a9-575b-46b4-870d-2988ede078c4.PNG">


`getMostWatchedCategoryForEachYear(spark, df)` &rarr; Renvoie un dataframe avec pour chaque année, le nom de la catégorie la plus visionnée ainsi que le nombre de vues cumulées

Exemple : 

<img width="300" alt="getMostWatchedCategoryForEachYear" src="https://user-images.githubusercontent.com/53021621/153436986-2479155f-9295-404e-8972-9acfa782703a.PNG">

`getVideosFromCategory(df, requestedCategory)` &rarr; Renvoie un dataframe de vidéos appartenant à une catégorie précise

`getMeanLikesDislikesPerXX(df, column_name)` &rarr; Renvoie un dataframe avec la moyenne de likes et de dislikes par groupe de colonne choisie spécifiquement

Exemple : `getMeanLikesDislikesPerXX(fr_videos, "category_name")`

<img width="300" alt="getMeanLikesDislikesPerXX" src="https://user-images.githubusercontent.com/53021621/153448345-77c5a9e9-4a2f-416a-977b-624429620d39.PNG">


`getBestRatioPerXX(df, column_name)` &rarr; Renvoie un dataframe avec la moyenne de likes, de dislikes, le ratio likes/dislikes, le % likes ainsi que le % de dislikes par groupe de colonne choisie spécifiquement

Exemple : `getBestRatioPerXX(fr_videos, "category_name")`

<img width="500" alt="getBestRatioPerXX" src="https://user-images.githubusercontent.com/53021621/153451108-7653733f-60c9-4642-b10b-b00226233770.PNG">


`doTrendingsVideosHaveDescription(df)` &rarr; -> Renvoie le % de vidéos en tendances qui n'ont pas de description

Exemple : `7% de vidéos n'ont pas de description`

`doMostViewedVideosAreTheMostCommentedOnes(df)` &rarr; -> Renvoie un dataframe avec les 100 vidéos les plus visionnées, leur titre, leur chaîne, leur categorie, leur nombre de commentaires et le % de commentaires par rapport à la moyenne (TO DO : A remplacer par la médiane) 

Exemple :

<img width="800" alt="doMostViewedVideosAreTheMostCommentedOnes" src="https://user-images.githubusercontent.com/53021621/153451542-b9134f0f-2efd-499c-9092-c54536d6b02b.PNG">

### A faire 

Pleins de fonctionnalités peuvent-être imaginées:
- Quelles sont les catégories qui reviennent le plus selon les pays / continent ? 
- etc.

# Setup

## Environnement 

* Le projet a été développé avec intellij IDEA
* Scala sdk 2.12.15
* Java sdk 11.0.5

## Languages - Frameworks utilisés 

* Spark
* Scala
