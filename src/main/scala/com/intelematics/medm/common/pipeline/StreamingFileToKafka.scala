package com.intelematics.medm.common.pipeline

import com.intelematics.medm.common.StreamingPipeline
import com.intelematics.medm.common.pipeline.processor.ProcessorBluePrint
import com.intelematics.medm.common.pipeline.sink.KafkaStreamingSink
import com.intelematics.medm.common.pipeline.source.RawFileStreamingSource
import com.intelematics.medm.utils.AppConfig

class StreamingFileToKafkaPipeline(configuration: AppConfig, processor: ProcessorBluePrint)
  extends StreamingPipeline(configuration, processor)
    with RawFileStreamingSource
    with KafkaStreamingSink {

  override def start(path: List[String]): Unit = None
}
