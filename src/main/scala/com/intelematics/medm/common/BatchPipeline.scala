package com.intelematics.medm.common

import com.intelematics.medm.common.pipeline.processor.ProcessorBluePrint
import com.intelematics.medm.common.pipeline.source.Source
import com.intelematics.medm.utils.AppConfig
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

abstract class BatchPipeline(config: AppConfig, processor: ProcessorBluePrint)
  extends AbstractPipelines[DataFrameWriter[Row]](config) with Source{

  private lazy val reader: DataFrame = createCustomReader(spark, config, schema=processor.createSchema())
  override protected lazy val processedPipelines: Array[DataFrame] = processor.defineProcessing(reader, spark)

  override def createWriter(pipelines: Array[DataFrame]): Array[DataFrameWriter[Row]] = createCustomWriter(pipelines)

  def consoleWriter(pipelines: Array[DataFrame]): Unit = {
    pipelines.foreach(df => df.show)
  }

  override def start(paths: List[String]): Unit = {
    if (configuration.getBoolean("spark.consoleWriter")) consoleWriter(processedPipelines)
    else writer.zip(paths).foreach(x => x._1.save(x._2))
  }

}
