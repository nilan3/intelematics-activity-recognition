package com.intelematics.medm.common

import com.intelematics.medm.utils.AppConfig
import com.intelematics.medm.common.pipeline.sink.Sink
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class AbstractPipelines[T](config: AppConfig)
  extends Sink[T] with SparkSessionHelper {

  protected val configuration: AppConfig = config
  protected val spark: SparkSession = createSpark(configuration)
  spark.sparkContext.setLogLevel("WARN")
  protected val processedPipelines: Array[DataFrame]
  protected lazy val writer: Array[T] = createWriter(processedPipelines)

  def createWriter(pipelines: Array[DataFrame]): Array[T]

  def start(path: List[String]): Unit

  def startStreaming(timeout: Int = 0): Unit

}
