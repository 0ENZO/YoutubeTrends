package readers

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileNotFoundException

class JsonReader(sparkSession: SparkSession) {

  private val schema = StructType(
    List(
      StructField("etag", StringType, nullable = true),
      StructField("id", IntegerType, nullable = false),
      StructField("kind", StringType, nullable = true),
      StructField("snippet", StructType(
        List(
          StructField("assignable", BooleanType, nullable = true),
          StructField("channelId", StringType, nullable = true),
          StructField("title", StringType, nullable = true)
        )
      ), nullable = true)

    )
  )

  @throws(classOf[FileNotFoundException])
  def read(filePath: String): DataFrame = {
    if (!filePath.endsWith(".json")) {
      throw new FileNotFoundException
    }
    val df: DataFrame = sparkSession
      .read
//      .schema(schema)
      .option("multiline", "true")
      .option("inferSchema", "true")
      //      .option("header" , "true")
      .json(filePath)


    println(s"${filePath}'s schema : ")
    df.printSchema()
    //    df.show()

    df
  }
}
