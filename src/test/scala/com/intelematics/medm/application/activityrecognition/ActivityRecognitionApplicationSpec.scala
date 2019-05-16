package com.intelematics.medm.application.activityrecognition

import com.intelematics.medm.common.SparkSessionHelper
import com.intelematics.medm.utils.{AppConfig, Utilities}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class ActivityRecognitionApplicationSpec extends FunSuite with SparkSessionHelper {
  private val configuration: AppConfig = Utilities.loadConfig(Array("configurations/telematics_activity_recognition.yml"))
  protected val spark: SparkSession = createSpark(configuration)
  protected val processor = new ActivityRecognitionApplication(configuration)

  private def buildTestDf(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read.parquet(path)
    df
  }

  test("test vector magnitude calculation sqr(x^2 + y^2 = z^2)") {
    val input_x = "2.2153"
    val input_y = "8.27915"
    val input_z = "5.58753"
    val expected = BigDecimal(10.2309515840)
    val actual = processor.sqrtBigDec(input_x, input_y, input_z)

  }

  test("test forward fill on Dataframe") {
    val input_df = buildTestDf(spark, "src/test/resources/data/activity-recognition/aggregation/input")
    val expected = buildTestDf(spark, "src/test/resources/data/activity-recognition/aggregation/expected").collect().toSet
    val actual = processor.agg(input_df, 11).coalesce(1).collect().toSet
    assert(actual == expected)
  }

}
