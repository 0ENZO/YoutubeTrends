package readers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvReader(sparkSession : SparkSession){

  val Schema: StructType = StructType(Array(
    StructField("video_id",StringType,nullable = true),
    StructField("trending_date",StringType,nullable = true),
    StructField("title",StringType,nullable = true),
    StructField("channel_title",StringType,nullable = true),
    StructField("category_id",StringType,nullable = true),
    StructField("publish_time",StringType,nullable = true),
    StructField("tags",StringType,nullable = true),
    StructField("views",IntegerType,nullable = true),
    StructField("likes",IntegerType,nullable = true),
    StructField("dislikes",IntegerType,nullable = true),
    StructField("comment_count",IntegerType,nullable = true),
    StructField("thumbnail_link",StringType,nullable = true),
    StructField("comments_disabled",BooleanType,nullable = true),
    StructField("ratings_disabled",BooleanType,nullable = true),
    StructField("video_error_or_removed",BooleanType,nullable = true),
    StructField("description",StringType,nullable = true),
  ))

  def read(filePath : String) : DataFrame = {
    val df: DataFrame = sparkSession
      .read
      .option("header" , "true")
      //.option("inferSchema" , "true")
      .schema(Schema)
      .csv(filePath)

    //println(s"${filePath}'s schema : ")
    df.withColumn("likes",col("likes").cast("int"))
    df.withColumn("dislikes",col("likes").cast(IntegerType))
    //df.printSchema()
    //df.show()
    df
  }
}
