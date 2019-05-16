package com.intelematics.medm.common.pipeline.source

import com.intelematics.medm.utils.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

trait RawFileStreamingSource {

  protected def createCustomReader(spark: SparkSession, configuration: AppConfig, schema: StructType): DataFrame = {
    val inputStream = spark.readStream
      .format("csv")
      .option("maxFilesPerTrigger", configuration.getString("hdfs.maxFilesPerTrigger"))
      .option("header", "false")
      .option("delimiter", ",")
      .schema(schema)

    inputStream.load(configuration.getString("hdfs.input"))
  }
}
