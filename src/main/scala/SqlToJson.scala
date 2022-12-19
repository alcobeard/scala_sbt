import org.apache.spark.sql.functions.{col, explode, first, monotonically_increasing_id, when}
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
      Row(545863491123L, Map("A07.01591.SberSpasibo_turn_off" -> 20.42, "A23.01639.Exchange_of_coins_for_banknotes" -> 20.42)),
      Row(545863491124L, Map("A23.01639.Exchange_of_coins_for_banknotes" -> 20.42)),
      Row(545863491125L, Map("A02.01575.Reasons_for_loan_arrears" -> 20.42)),
      Row(545863491126L, Map("A31.12685.Bankruptcy" -> 20.42, "brak_osnovnoy_infoquiz" -> 20.42)),
    )

    val mapType = DataTypes.createMapType(StringType,DoubleType)

    val arrayStructureSchema = new StructType()
      .add("epk_id", LongType)
      .add("calibrated_score_array", mapType)

    val mapTypeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData),arrayStructureSchema)
    mapTypeDF.printSchema()
    mapTypeDF.show()

    val explodeDF = mapTypeDF.select($"epk_id", explode($"calibrated_score_array"))
      .withColumn("system_name", when(col("key") === "A07.01591.SberSpasibo_turn_off", "SberSpasibo_turn_off")
        when(col("key") === "A23.01639.Exchange_of_coins_for_banknotes", "Exchange_of_coins_for_banknotes")
        when(col("key") === "A02.01575.Reasons_for_loan_arrears", "loans_general_info")
        when(col("key") === "A31.12685.Bankruptcy", "Bankruptcy")
        when(col("key") === "brak_osnovnoy_infoquiz", "gosuslugi")
      )
      .withColumnRenamed("key","intent")
      .withColumnRenamed("value","score")
//      .groupBy("epk_id")
//      .pivot("key")
//      .agg(first("value")).show()
    explodeDF.printSchema()
    explodeDF.show()



//    //From Data (USING createDataFrame)
//    val dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
//    dfFromData2.show()
  }
}
