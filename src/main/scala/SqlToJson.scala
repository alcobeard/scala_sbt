import org.apache.spark.sql.types.{DataTypes, DoubleType, LongType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.io._
import scala.io.BufferedSource
import scala.io.Source.fromFile
import scala.language.postfixOps

object SqlToJson {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("spark_local")
      .getOrCreate()

    import spark.implicits._

//    val columns = Seq("epk_id","calibrated_score_array")
//    val data = Seq(
//      (545863491123L, {"20000"; 20.43}),
//      (545863491123L, {"20000"; 20.43}),
//      (545863491123L, {"20000"; 20.43})
//    )

    var structureData = Seq(
      Row(545863491123L, Map("intent,system_name" -> 20.42)),
      Row(545863491124L, Map("intent,system_name" -> 20.42)),
      Row(545863491125L, Map("intent,system_name" -> 20.42)),
      Row(545863491126L, Map("intent,system_name" -> 20.42)),
    )

    val mapType = DataTypes.createMapType(StringType,DoubleType)

    val arrayStructureSchema = new StructType()
      .add("epk_id", LongType)
      .add("calibrated_score_array", mapType)

    val mapTypeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData),arrayStructureSchema)
    mapTypeDF.printSchema()
    mapTypeDF.show()

//    //From Data (USING createDataFrame)
//    val dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
//    dfFromData2.show()
  }
}
