package readers

import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonReader(sparkSession : SparkSession) {

  def read(filePath : String) : DataFrame = {
    val df: DataFrame = sparkSession
      .read
      .option("header" , "true")
      .option("inferSchema" , "true")
      .json(filePath)


    df.printSchema()
    df.show()

    df
  }
}
