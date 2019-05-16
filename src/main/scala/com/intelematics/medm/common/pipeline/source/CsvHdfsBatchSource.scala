package com.intelematics.medm.common.pipeline.source

import com.intelematics.medm.utils.AppConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CsvHdfsBatchSource {

  protected def createCustomReader(spark: SparkSession, configuration: AppConfig, schema: StructType): DataFrame = {
    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .load(configuration.getString("hdfs.input"))
  }

}
