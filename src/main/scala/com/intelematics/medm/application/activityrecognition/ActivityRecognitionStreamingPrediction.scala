package com.intelematics.medm.application.activityrecognition

import com.intelematics.medm.common.pipeline.StreamingFileToKafkaPipeline
import com.intelematics.medm.common.pipeline.processor.ProcessorBluePrint
import com.intelematics.medm.utils.{AppConfig, Utilities}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class ActivityRecognitionStreamingPrediction(configuration: AppConfig)
  extends ProcessorBluePrint with Serializable {

  override def createSchema(): StructType = StructType(List(
    StructField("timestamp", DoubleType, nullable = false),
    StructField("activity_id", StringType, nullable = false),
    StructField("heart_rate", DecimalType(10, 2), nullable = true),
    StructField("wrist_accelerometer_x", DecimalType(15,10), nullable = true),
    StructField("wrist_accelerometer_y", DecimalType(15,10), nullable = true),
    StructField("wrist_accelerometer_z", DecimalType(15,10), nullable = true),
    StructField("wrist_gyroscope_x", DecimalType(15,10), nullable = true),
    StructField("wrist_gyroscope_y", DecimalType(15,10), nullable = true),
    StructField("wrist_gyroscope_z", DecimalType(15,10), nullable = true),
    StructField("wrist_magnetometer_x", DecimalType(15,10), nullable = true),
    StructField("wrist_magnetometer_y", DecimalType(15,10), nullable = true),
    StructField("wrist_magnetometer_z", DecimalType(15,10), nullable = true),
    StructField("chest_accelerometer_x", DecimalType(15,10), nullable = true),
    StructField("chest_accelerometer_y", DecimalType(15,10), nullable = true),
    StructField("chest_accelerometer_z", DecimalType(15,10), nullable = true),
    StructField("chest_gyroscope_x", DecimalType(15,10), nullable = true),
    StructField("chest_gyroscope_y", DecimalType(15,10), nullable = true),
    StructField("chest_gyroscope_z", DecimalType(15,10), nullable = true),
    StructField("chest_magnetometer_x", DecimalType(15,10), nullable = true),
    StructField("chest_magnetometer_y", DecimalType(15,10), nullable = true),
    StructField("chest_magnetometer_z", DecimalType(15,10), nullable = true),
    StructField("ankle_accelerometer_x", DecimalType(15,10), nullable = true),
    StructField("ankle_accelerometer_y", DecimalType(15,10), nullable = true),
    StructField("ankle_accelerometer_z", DecimalType(15,10), nullable = true),
    StructField("ankle_gyroscope_x", DecimalType(15,10), nullable = true),
    StructField("ankle_gyroscope_y", DecimalType(15,10), nullable = true),
    StructField("ankle_gyroscope_z", DecimalType(15,10), nullable = true),
    StructField("ankle_magnetometer_x", DecimalType(15,10), nullable = true),
    StructField("ankle_magnetometer_y", DecimalType(15,10), nullable = true),
    StructField("ankle_magnetometer_z", DecimalType(15,10), nullable = true),
    StructField("user_id", StringType, nullable = true)
  ))

  override def defineProcessing(data: DataFrame, spark: SparkSession): Array[DataFrame] = {
    """
      Apply standardizastion on streaming data and use model to predict activity
      NOTE: MultilayerPerceptronClassifier is NOT currently supported in structured streaming
      however we can use databricks sparkdl by building keras model and forming a transformer
      to apply to streaming context
    """.stripMargin
//    val scaler = StandardScaler.load(configuration.getString("hdfs.scaler"))
//    val model = MultilayerPerceptronClassifier.load(configuration.getString("hdfs.model"))
    Array(data)
  }

}

object ActivityRecognitionStreamingPrediction {

  def createProcessor(configuration: AppConfig): ProcessorBluePrint = new ActivityRecognitionStreamingPrediction(configuration)

  def main(args: Array[String]): Unit = {
    val configuration = Utilities.loadConfig(args)
    val pipeline: Unit = new StreamingFileToKafkaPipeline(configuration, createProcessor(configuration)).startStreaming()
  }
}