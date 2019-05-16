package com.intelematics.medm.common.pipeline.sink

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

trait KafkaStreamingSink extends Sink[DataStreamWriter[Row]] {
  override def createCustomWriter(pipelines: Array[DataFrame]): Array[DataStreamWriter[Row]] = {
    val streams = pipelines.zipWithIndex.map { case (pipeline, index) =>
      pipeline.writeStream
        .format("kafka")
        .outputMode(configuration.getString("spark.outputMode"))
        .option("kafka.bootstrap.servers", configuration.getString("kafka.bootstrap_servers"))
        .option("checkpointLocation", configuration.getString("spark.checkpointLocation") + "/" + index.toString)
        .option("kafka.compression.type", configuration.getString("kafka.compression"))
    }
    streams
  }
}
