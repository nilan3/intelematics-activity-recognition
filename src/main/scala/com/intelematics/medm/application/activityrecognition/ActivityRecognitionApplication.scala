package com.intelematics.medm.application.activityrecognition

import com.intelematics.medm.common.aggregation.MultiAggregation
import com.intelematics.medm.common.pipeline.BatchCsvToHdfsPipeline
import com.intelematics.medm.common.pipeline.processor.ProcessorBluePrint
import com.intelematics.medm.utils.{AppConfig, Utilities}

import java.math.{BigDecimal => JBigDecimal}
import java.math.MathContext
import scala.annotation.tailrec
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ActivityRecognitionApplication(configuration: AppConfig)
  extends ProcessorBluePrint
    with Serializable
    with MultiAggregation {

  override def defineProcessing(data: DataFrame, spark: SparkSession): Array[DataFrame] = {
    """
      Main Pipeline Processor
    """.stripMargin
    val ordered = data.orderBy(col("user_id"), col("timestamp").asc)
    val removedEmptyRows = removeEmptyRows(ordered)
    val simplifyImuData = simplifyImuMeasurements(removedEmptyRows)
    // df.coalesce(1).write.mode("overwrite").parquet("")
    println(simplifyImuData.count()) // 14326 rows contains nulls in atleast one imu metric
    val result = aggregateResults(simplifyImuData, spark).orderBy(col("timestamp").asc)
    result.select("user_id").distinct().show()
    Array(result)
  }

  def removeEmptyRows(dataFrame: DataFrame): DataFrame = {
    """
      Remove rows where all measurements are 'NA'
      Currently showing 40 / 1158399 rows need removing
    """.stripMargin
    val removeCols = List("timestamp", "activity_id", "user_id")
    val measurementCols = dataFrame.columns.filter(!removeCols.contains(_))
    dataFrame
      .withColumn("isNull", concat_ws(",",measurementCols.map(c => col(c)): _*))
      .where(col("isNull") =!= List.fill(measurementCols.length)("NA").mkString(","))
      .drop("isNull")
  }

  def sqrtBigDec(x: String, y: String, z: String): BigDecimal = {
    """
      Work around for calculating square root of Big Decimals
    """.stripMargin
    def sqrt(a: JBigDecimal, scale: Int): JBigDecimal = {
      var x = new JBigDecimal( Math.sqrt(a.doubleValue()), MathContext.DECIMAL64 )
      if (scale < 17) {
        return x
      }
      val b2 = new JBigDecimal(2)
      var tempScale = 16
      while(tempScale < scale){
        //x = x - (x * x - a) / (2 * x);
        x = x.subtract(x.multiply(x).subtract(a).divide(x.multiply(b2), scale, JBigDecimal.ROUND_HALF_EVEN))
        tempScale *= 2
      }
      x
    }
    if (x != "NA"){
      val squaredSum = ((BigDecimal(x) * BigDecimal(x)) + (BigDecimal(y) * BigDecimal(y)) + (BigDecimal(z) * BigDecimal(z))).toString()
      val magnitude = BigDecimal(sqrt(new JBigDecimal(squaredSum), 2)).setScale(10, BigDecimal.RoundingMode.HALF_UP)
      magnitude
    } else {
      null
    }
  }

  def simplifyImuMeasurements(df: DataFrame): DataFrame = {
    """
       IMU measurement are composed of X,Y,Z components.
       This combine can be treated as a vector and magnitude can be calculated to use as a single value.
       Aiming to reduce data to avoid large storage
    """.stripMargin
    val body = List("wrist", "chest", "ankle")
    val sensors = List("accelerometer", "gyroscope", "magnetometer")
    val metrics = body.flatMap(x => List(s"${x}_${sensors.head}", s"${x}_${sensors(1)}", s"${x}_${sensors(2)}"))

    val magnitude3D = udf((x: String, y: String, z: String) => {
      sqrtBigDec(x, y, z)
    })
    @tailrec
    def createMetricColumns(dataframe: DataFrame, metricList: List[String]): DataFrame = {
      if (metricList.nonEmpty) {
        val metric = metricList.head
        val newDf = dataframe
          .withColumn(metric, magnitude3D(col(s"${metric}_x"),
            col(s"${metric}_y"), col(s"${metric}_z")).cast(DecimalType(15,10)))
          .drop(s"${metric}_x")
          .drop(s"${metric}_y")
          .drop(s"${metric}_z")
        createMetricColumns(newDf, metricList.tail)
      } else dataframe
    }
    createMetricColumns(df, metrics)
  }

  def aggregateResults(dataFrame: DataFrame, spark: SparkSession): DataFrame = {
    """
      Split dataframe so data is separated per user-activity combination
      Once split, each block will be aggregated based on HR frequency and every row will be ensure to contain
      all IMU measurements.
    """.stripMargin
    val preparedDf = dataFrame
      .withColumn("ts", round(col("timestamp")*100, 0).cast(IntegerType))
      .withColumn("user_activity", concat(col("user_id"), lit("_"),
        col("activity_id")))
      .withColumn("heart_rate", when(col("heart_rate") === "NA", null)
        .otherwise(col("heart_rate").cast(IntegerType)))

    val uniqUserActivities: List[String] = preparedDf.select("user_activity").distinct.collect()
      .map(_(0).toString).toList
    val aggRows = math.round(configuration.getInt("aggregation.imuFreq").toFloat /
      configuration.getInt("aggregation.hrFreq")) // 100/9 = 11
    preparedDf.cache()
    recursiveAggregationUserActivity(preparedDf, uniqUserActivities, spark.emptyDataFrame, aggRows)
  }

  @tailrec
  private def recursiveAggregationUserActivity(originalDf: DataFrame, uniqUserActivities: List[String], aggregatedDf: DataFrame, aggRows: Int): DataFrame = {
    if (uniqUserActivities.isEmpty) aggregatedDf
    else {
      val userActivity = uniqUserActivities.head
      val filtered = originalDf.where(col("user_activity") === userActivity)
      val remaining = originalDf.where(col("user_activity") =!= userActivity)
      val newAggDf = agg(filtered, aggRows)
      if (aggregatedDf.isEmpty) recursiveAggregationUserActivity(remaining, uniqUserActivities.tail, newAggDf, aggRows)
      else {
        recursiveAggregationUserActivity(remaining, uniqUserActivities.tail, aggregatedDf.union(newAggDf), aggRows)
      }
    }
  }

  def agg(dataFrame: DataFrame, aggRows: Int): DataFrame = {
    """
      aggregated on HR frequency so each aggregation bucket will contain a HR measurement
      IMU measurements averaged. Any bucket with all NULLs will be removed.
    """.stripMargin
    val groupCol = "group_index"
    val nonAggColumns = List("user_id", "activity_id", "user_activity")
    val groupedDf = dataFrame
      .withColumn(groupCol, (col("ts")/aggRows).cast(IntegerType))
    val res = avg_agg(groupedDf, aggRows, groupCol, nonAggColumns)
      .drop("group_index")
      .drop("user_activity")
      .na.drop()
      .withColumn("timestamp", col("timestamp").cast(DecimalType(10, 2)))
      .select("timestamp", "user_id", "activity_id", "heart_rate",
        "wrist_accelerometer", "wrist_gyroscope", "wrist_magnetometer",
        "chest_accelerometer", "chest_gyroscope", "chest_magnetometer",
        "ankle_accelerometer", "ankle_gyroscope", "ankle_magnetometer")
    res
  }

  override def createSchema(): StructType = StructType(List(
    StructField("timestamp", DoubleType, nullable = false),
    StructField("activity_id", StringType, nullable = false),
    StructField("heart_rate", StringType, nullable = true),
    StructField("wrist_accelerometer_x", StringType, nullable = true),
    StructField("wrist_accelerometer_y", StringType, nullable = true),
    StructField("wrist_accelerometer_z", StringType, nullable = true),
    StructField("wrist_gyroscope_x", StringType, nullable = true),
    StructField("wrist_gyroscope_y", StringType, nullable = true),
    StructField("wrist_gyroscope_z", StringType, nullable = true),
    StructField("wrist_magnetometer_x", StringType, nullable = true),
    StructField("wrist_magnetometer_y", StringType, nullable = true),
    StructField("wrist_magnetometer_z", StringType, nullable = true),
    StructField("chest_accelerometer_x", StringType, nullable = true),
    StructField("chest_accelerometer_y", StringType, nullable = true),
    StructField("chest_accelerometer_z", StringType, nullable = true),
    StructField("chest_gyroscope_x", StringType, nullable = true),
    StructField("chest_gyroscope_y", StringType, nullable = true),
    StructField("chest_gyroscope_z", StringType, nullable = true),
    StructField("chest_magnetometer_x", StringType, nullable = true),
    StructField("chest_magnetometer_y", StringType, nullable = true),
    StructField("chest_magnetometer_z", StringType, nullable = true),
    StructField("ankle_accelerometer_x", StringType, nullable = true),
    StructField("ankle_accelerometer_y", StringType, nullable = true),
    StructField("ankle_accelerometer_z", StringType, nullable = true),
    StructField("ankle_gyroscope_x", StringType, nullable = true),
    StructField("ankle_gyroscope_y", StringType, nullable = true),
    StructField("ankle_gyroscope_z", StringType, nullable = true),
    StructField("ankle_magnetometer_x", StringType, nullable = true),
    StructField("ankle_magnetometer_y", StringType, nullable = true),
    StructField("ankle_magnetometer_z", StringType, nullable = true),
    StructField("user_id", StringType, nullable = true)
  ))
}

object ActivityRecognitionApplication {

  def createProcessor(configuration: AppConfig): ProcessorBluePrint = new ActivityRecognitionApplication(configuration)

  def main(args: Array[String]): Unit = {
    val configuration: AppConfig = Utilities.loadConfig(args)
    val outputPaths: List[String] = configuration.getList("hdfs.output")
    val pipeline: Unit = new BatchCsvToHdfsPipeline(configuration, createProcessor(configuration))
      .start(outputPaths)
  }
}