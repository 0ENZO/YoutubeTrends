package App

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import readers.{CsvReader, JsonReader}
import java.io.{File, FileNotFoundException}
import org.apache.spark.sql.types.DoubleType
import scala.sys.exit


object Application {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TP1")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val lang_code_to_continent = Map[String, String](
      "FR" -> "Europe",
      "DE" -> "Europe",
      "GB" -> "Europe",
      "CA" -> "North_America",
      "US" -> "North_America",
      "MX" -> "North_America",
      "IN" -> "Asia",
      "JP" -> "Asia",
      "KR" -> "Asia",
      "RU" -> "Russia"
    )
    val dfCategories = readCategoriesFile(spark)
    val fr_videos = mergeVideosWithCategories(readVideosFile(spark, "FR"), dfCategories)

    var globalVideosDf = getGlobalVideosDf(spark, lang_code_to_continent)
    //globalVideosDf.filter(row => row != globalVideosDf.first())
    globalVideosDf.show()
    println(s"Dataframe de ${globalVideosDf.count()} videos")

    println("dfCategories")
    dfCategories.show(1, truncate = false)

    println("mergeVideosWithCategories")
    mergeVideosWithCategories(fr_videos, dfCategories).show(1)

    val artist = "Anil B"
    println(s"getTotalViewsPerCategoryForSpecificChannel $artist")
    getTotalViewsPerCategoryForSpecificChannel(spark, fr_videos, artist).show()

    val yearRequested = "2018"
    println(s"getMostWatchedChannelsForSpecificYear $yearRequested")
    getMostWatchedChannelsForSpecificYear(fr_videos, yearRequested).show()

    println("getMostWatchedCategoryForEachYear")
    getMostWatchedCategoryForEachYear(spark, fr_videos).show()

    val requestedCategory = "1"
    println(s"getVideosFromCategory  : $requestedCategory")
    getVideosFromCategory(fr_videos, requestedCategory).show(1)

    println("getMeanLikesDislikesPerXX")
    getMeanLikesDislikesPerXX(fr_videos, "category_name").show()

    println("getBestRatioPerXX")
    getBestRatioPerXX(fr_videos , orderAsc = true).show()

    println("doTrendingsVideosHaveDescription")
    doTrendingsVideosHaveDescription(fr_videos)

    println("doMostViewedVideosAreTheMostCommentedOnes")
    doMostViewedVideosAreTheMostCommentedOnes(fr_videos)
  }

  def readVideosFile(spark: SparkSession, lang: String): (DataFrame) = {
    try {
      val df = new CsvReader(spark).read(s"data/${lang}videos.csv")
      df
    }
    catch {
      case _: FileNotFoundException =>
        println("The requested file surely does not exist")
        exit(1)
    }
  }

  def readCategoriesFile(spark: SparkSession): DataFrame = {
    try {
      val df = new JsonReader(spark).read("data/category_id.json")

      //      val df2 = new CsvReader(spark).read("data/%svideos.csv".format(lang))
      df
    }
    catch {
      case _: FileNotFoundException =>
        println("The requested file surely does not exist")
        exit(1)
    }
  }

  def mergeVideosWithCategories(dfVideos: DataFrame, dfCategories: DataFrame): DataFrame = {
    dfVideos
      .join(
        dfCategories,
        dfVideos("category_id") === dfCategories("id")
      )
      .withColumn("category_name",
        functions.reverse(
          functions.split(
            translate(col("snippet").cast("String"), "{} ", "\0\0\0"),
            ","
          )
        ).getItem(0)
      )
      .drop("etag", "id", "kind", "snippet", "video_id", "thumbnail_link", "tags" )
  }

  def getVideosFromCategory(dfVideos: DataFrame, category_id: String): DataFrame = {
    val videosOfCategory1 = dfVideos.filter(s"category_id == ${category_id}")

    println(s"filtered ${videosOfCategory1.count()} videos")
    //    videosOfCategory1.foreach(x => {
    //      println(x.getAs[String]("title"))
    //    })
    videosOfCategory1
  }

  def getMeanLikesDislikesPerXX(dfVideos: DataFrame, columnToGroup: String): DataFrame = {
    val dfLikes = dfVideos
      .withColumnRenamed(columnToGroup, columnToGroup + "_tmp")
      .groupBy(columnToGroup + "_tmp")
      .mean(col("dislikes").toString()).as("Moyenne de dislikes").na.drop()

    val dfDislikes = dfVideos.groupBy(columnToGroup)
      .mean("likes").as("Moyenne de likes").na.drop()

    dfLikes
      .join(
        dfDislikes,
        dfDislikes(columnToGroup) === dfLikes(columnToGroup + "_tmp")
      ).drop(columnToGroup + "_tmp")
  }

  def getBestRatioPerXX(dfVideos: DataFrame, columnToGroup: String = "category_name", orderAsc: Boolean = false): DataFrame = {
    val newColumn = "ratio UP/DOWN"
    val newColumnLikes = "proportion de likes"
    val newColumnDislikes = "proportion de dislikes"
    getMeanLikesDislikesPerXX(dfVideos, columnToGroup)
      .withColumn(newColumn, format_number(col("Moyenne de likes.avg(likes)") / (col("Moyenne de dislikes.avg(dislikes)") + 0.01), 2).cast("Int"))
      .withColumn(newColumnLikes , format_number( (col("Moyenne de likes.avg(likes)") / (col("Moyenne de likes.avg(likes)") + col("Moyenne de dislikes.avg(dislikes)")) ) * 100 , 2))
      .withColumn(newColumnDislikes , format_number( (col("Moyenne de dislikes.avg(dislikes)") / (col("Moyenne de likes.avg(likes)") + col("Moyenne de dislikes.avg(dislikes)")) ) * 100 , 2))
      .sort(if (orderAsc) asc(newColumn) else {
        desc(newColumn)
      })
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def createEmptyDF(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val emptyData = Seq(("", "", "", "", "", "", "", 0, 0, 0, 0, "", true, true, true, "", "", ""))
    val df = emptyData.toDF(
      "video_id", "trending_date", "title", "channel_title", "category_id", "publish_time",
      "tags", "views", "likes", "dislikes", "comment_count", "thumbnail_link", "comments_disabled",
      "ratings_disabled", "video_error_or_removed", "description", "lang_code", "continent")

    df
  }

  def getGlobalVideosDf(spark: SparkSession, lang_code_to_continent: Map[String, String]): DataFrame = {
    val x = getListOfFiles(s"data/")
    var main_df = createEmptyDF(spark)

    x.foreach(e =>
      if (e.toString.endsWith("v")) { // .csv
        val lang_code = e.toString.substring(5, 7)
        if(lang_code != "FR") {
          println(s"Fichier $lang_code en cours..")
          val df = new CsvReader(spark).read(s"data/${lang_code}videos.csv")
          val df2 = df
            .withColumn("lang_code", lit(lang_code))
            .withColumn("continent", lit(lang_code_to_continent(lang_code)))

          main_df = main_df.union(df2)
        }
      })
    //val final_df = main_df.filter(row => row != main_df.first())
    main_df
  }

  def doTrendingsVideosHaveDescription(dfVideos: DataFrame) = {
    println((dfVideos.filter(col("description").isNull || col("description") === "").count().toDouble / dfVideos.count().toDouble * 100).round + "% de vidéos n'ont pas de description")
  }

  def doMostViewedVideosAreTheMostCommentedOnes(dfVideos : DataFrame) = {
    val df = dfVideos
      .filter(col("comment_count") > lit(0))
      .orderBy(desc("views"))
      .limit(100)

    val commentsAvg = df
      .select(avg("comment_count")).collect()(0)(0).toString().toDouble // A remplacer par la médiane

    println("Moyenne du nombre des commentaires des 100 vidéos les plus vues : " + commentsAvg)

    df
      .select("title", "channel_title", "views", "comment_count", "category_name")
      .withColumn("comment_count", round(col("comment_count").cast(DoubleType), 2))
      .withColumn("Pourcentage de commentaires par rapport à la moyenne", concat_ws("", round(((col("comment_count") - commentsAvg) / commentsAvg) * 100, 2), lit("%")))
      .show()
  }

  def getTotalViewsPerCategoryForSpecificChannel(spark: SparkSession, dfVideos: DataFrame, yt_channel_title: String): DataFrame = {
    import spark.implicits._

    val df = dfVideos
      .filter($"channel_title" === yt_channel_title)
      .groupBy("channel_title", "category_name")
      .agg(sum($"views").as("total_views"), count($"category_name").as("cpt"))
      .select(concat_ws("", $"category_name", lit(" ("), $"cpt", lit(")")).as("Nombre de vues par catégorie"), $"total_views")

    df
  }

  def getMostWatchedChannelsForSpecificYear(dfVideos: DataFrame, specific_year: String): DataFrame = {

    dfVideos
      .filter(year(col("publish_time")).geq(lit(specific_year)))
      .groupBy("channel_title")
      .agg(sum("views").as("total_views"))
      .sort(desc("total_views"))
  }

  def getMostWatchedCategoryForEachYear(spark: SparkSession, dfVideos: DataFrame): DataFrame = {
    import spark.implicits._

    val df = dfVideos
      .na.drop()
      .groupBy(year(col("publish_time")).as("year"), col("category_name"))
      .agg(sum("views").as("total_views"))
      .sort(year(col("publish_time")))

    val windowDept = Window.partitionBy(col("year")).orderBy(col("total_views").desc)

    df.withColumn("row", row_number.over(windowDept))
      .where($"row" === 1).drop("row")
  }

  /*
  def getMostTrendingChannels(dfVideos : DataFrame): Unit = {
    val df = dfVideos
      .groupBy("channel_title")
      .agg(sum("views")).as("total_views")
      .orderBy("total_views")
      .show(15)

    //val df = dfVideos.agg(sum("views").as("total_views"), sum("col2").as("sum_col2"), ...).first
  }*/
}

