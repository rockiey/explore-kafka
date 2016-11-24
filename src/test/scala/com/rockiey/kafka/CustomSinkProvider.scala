package com.knockdata.redback.kafka

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

class CustomSinkProvider extends StreamSinkProvider {
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    new Sink {
      override def addBatch(batchId: Long, data: DataFrame): Unit = {
        data.printSchema()

        data.show()
        println(s"count ${data.count()}")
      }
    }
  }
}
