package App

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import readers.{CsvReader, JsonReader}
import java.io.File
import java.io.FileNotFoundException
import org.apache.spark.sql.types.DateType


import scala.sys.exit
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.expressions.Window


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
    val globalVideosDf = getGlobalVideosDf(spark, lang_code_to_continent) // Supprimer première ligne
    // globalVideosDf.filter(row => row != globalVideosDf.first())
    globalVideosDf.show()

    val FR_df = readVideosFile(spark, "FR")
    // getMostTrendingChannels(FR_df)
    // getTotalViewsPerCategoryForSpecificChannel(spark, FR_df, "GMK")
    // getCategoryMostWatchForSpecificYear(FR_df, "2018")
    // getMostWatchedCategoryForEachYear(spark, FR_df)
    // getMostWatchedChannelsForSpecificYear(FR_df, "2018")
    // getMostTrendingsVideos(FR_df)

    sys.exit(0)

    val LANGUAGE = "FR"
    val (dfCategories, dfVideos) = readFiles(spark, LANGUAGE)

    println("dfCategories")
    dfCategories.show(1, truncate = false)

    println("dfVideos")
    dfVideos.show(1, truncate = false)

    getVideosFromCategory(dfVideos , dfCategories.collectAsList().get(0).getAs[String]("id")).show(10)
    meanDislikesPerCategory(dfVideos, dfCategories)
    mergeVideosWithCategories(dfVideos, dfCategories)
  }

  def readVideosFile(spark: SparkSession , lang : String): (DataFrame) = {
    try {
      val df = new CsvReader(spark).read(s"data/${lang}videos.csv")
      (df)
    }
    catch {
      case _: FileNotFoundException =>
        println("The requested file surely does not exist")
        exit(1)
    }
  }

  def readFiles(spark: SparkSession , lang : String): (DataFrame, DataFrame) = {
    try {
      val df = new JsonReader(spark).read(s"data/category_id.json")

      val df2 = new CsvReader(spark).read(s"data/${lang}videos.csv")
      (df, df2)
    }
    catch {
      case _: FileNotFoundException =>
        println("The requested file surely does not exist")
        exit(1)
    }
  }

  def mergeVideosWithCategories(dfVideos : DataFrame, dfCategories : DataFrame): DataFrame = {
    val test =dfVideos
      .join(
        dfCategories,
        dfVideos("category_id") === dfCategories("id")
      ).toDF()
    test.show(false)
    test
  }

  def getVideosFromCategory(dfVideos : DataFrame, category_id : String ): DataFrame = {
    println(s"requested categ : $category_id")
    val videosOfCategory1 = dfVideos.filter(s"category_id == ${category_id}")
    println(s"filtered ${videosOfCategory1.count()} videos")
    println("AIE !!!! " , dfVideos.agg(functions.max("category_id")).show)
    videosOfCategory1
  }

  def meanDislikesPerCategory(dfVideos : DataFrame, dfCategories : DataFrame): DataFrame = {
    val dfVidsSorted = dfVideos.groupBy("category_id").mean("dislikes").drop()
    dfVidsSorted.show(truncate = false , numRows = 20)
    dfVidsSorted
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def createEmptyDF(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val emptyData = Seq(("", "", "", "", "", "", "", "", "", "", "", "", true, true, true, "", "", ""))
    val df = emptyData.toDF(
      "video_id","trending_date","title","channel_title","category_id","publish_time",
      "tags", "views", "likes", "dislikes", "comment_count", "thumbnail_link", "comments_disabled",
      "ratings_disabled", "video_error_or_removed", "description", "lang_code", "continent")

    return df
  }

  def getGlobalVideosDf(spark: SparkSession, lang_code_to_continent: Map[String, String]): DataFrame = {
    val x = getListOfFiles(s"data/")
    var main_df = createEmptyDF(spark)

    x.foreach(e =>
      if (e.toString().endsWith("v")){ // .csv
        val lang_code  = e.toString().substring(5, 7)
        println(s"Fichier ${lang_code} en cours..")
        val df = new CsvReader(spark).read(s"data/${lang_code}videos.csv")
        val df2 = df
          .withColumn("lang_code", lit(lang_code))
          .withColumn("continent", lit(lang_code_to_continent.get(lang_code).get))
        main_df = main_df.union(df2)
        main_df.show()
      })
    //val final_df = main_df.filter(row => row != main_df.first())
    main_df
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

  def getTotalViewsPerCategoryForSpecificChannel(spark: SparkSession, dfVideos : DataFrame, yt_channel_title : String): Unit = {
    import spark.implicits._

    val df = dfVideos
      .filter($"channel_title" === yt_channel_title)
      .groupBy("channel_title", "category_id")
      .agg(sum($"views").as("total_views"), count($"category_id").as("cpt"))
      .select(concat_ws("", $"category_id", lit(" ("), $"cpt", lit(")")).as("Nombre de vues par catégories"), $"total_views")

    df.show()
  }

  def getMostWatchedChannelsForSpecificYear(dfVideos : DataFrame, specific_year : String) = {

    val df = dfVideos
      .filter(year(col("publish_time")).geq(lit(specific_year)))
      .groupBy("channel_title")
      .agg(sum("views").as("total_views"))
      .sort(desc("total_views"))
      .show()
  }

  def getMostWatchedCategoryForEachYear(spark: SparkSession, dfVideos : DataFrame): Unit = {
    import spark.implicits._

    val df = dfVideos
      .na.drop()
      .groupBy(year(col("publish_time")).as("year"), col("category_id"))
      .agg(sum("views").as("total_views"))
      .sort(year(col("publish_time")))

    val windowDept = Window.partitionBy(col("year")).orderBy(col("total_views").desc)
    df.withColumn("row",row_number.over(windowDept))
      .where($"row" === 1).drop("row")
      .show()
  }

  def getMostTrendingsVideos(dfVideos: DataFrame): Unit ={

    /*
    val df = dfVideos
      .select("title", "channel_title", "category_id", "views", "trending_date", "publish_time")
      .withColumn("publish_time", df("publish_time").cast(DateType))
      //.withColumn("publish_time", date_format(to_date(col("publish_time"),"MM/dd/yyyy"), "yyyyMMdd"))
      //.withColumn("days_diff", datediff(col("trending_date"), col("publish_time" )))
      .sort(desc("days_diff"))
      .show()
    */
  }
}

