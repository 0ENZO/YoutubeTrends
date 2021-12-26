package readers

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileNotFoundException

class JsonReader(sparkSession : SparkSession) {

  @throws(classOf[FileNotFoundException])
  def read(filePath : String) : DataFrame = {
    if ( !filePath.endsWith(".json") ){
      throw new FileNotFoundException
    }
    val df: DataFrame = sparkSession
      .read
//      .option("header" , "true")
      .option("multiline","true")
      .option("inferSchema" , "true")
      .json(filePath)


    df.printSchema()
    df.show()

    df
  }
}
