package com.intelematics.medm.common.pipeline

import com.intelematics.medm.common.BatchPipeline
import com.intelematics.medm.common.pipeline.processor.ProcessorBluePrint
import com.intelematics.medm.common.pipeline.sink.HdfsBatchSink
import com.intelematics.medm.common.pipeline.source.CsvHdfsBatchSource
import com.intelematics.medm.utils.AppConfig

class BatchCsvToHdfsPipeline(configuration: AppConfig, processor: ProcessorBluePrint)
  extends BatchPipeline(configuration, processor)
    with CsvHdfsBatchSource
    with HdfsBatchSink {

  override def startStreaming(timeout: Int): Unit = None

}
