package com.intelematics.medm.common.pipeline.source

import com.intelematics.medm.utils.AppConfig
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Source {
  protected val configuration: AppConfig

  protected def createCustomReader(spark: SparkSession, configuration: AppConfig, schema: StructType): DataFrame
}
