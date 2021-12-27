package readers

import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvReader(sparkSession : SparkSession){

  def read(filePath : String) : DataFrame = {
    val df: DataFrame = sparkSession
      .read
      .option("header" , "true")
      .option("inferSchema" , "true")
      .csv(filePath)

    println(s"${filePath}'s schema : ")
    df.printSchema()
//    df.show()

    df
  }
}
