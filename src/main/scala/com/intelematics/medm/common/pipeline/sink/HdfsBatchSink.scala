package com.intelematics.medm.common.pipeline.sink

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

trait HdfsBatchSink extends Sink[DataFrameWriter[Row]] {

  override def createCustomWriter(pipelines: Array[DataFrame]): Array[DataFrameWriter[Row]] = {
    val dataWriters = pipelines.map(
      _.write.format("parquet").mode(SaveMode.Overwrite)
    )
    dataWriters
  }
}
