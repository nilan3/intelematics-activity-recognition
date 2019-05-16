package com.intelematics.medm.common.pipeline.processor

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class ProcessorBluePrint {

  def createSchema(): StructType

  def defineProcessing(data: DataFrame, spark: SparkSession): Array[DataFrame]

}
