package com.rockiey.kafka

import java.sql.Date

import com.com.knockdata.redback.SparkEnv
import org.junit.Test
import org.apache.spark.sql.functions._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

class TestSparkStreaming {
  @Test
  def testStreaming: Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream[String, String](
      SparkEnv.ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val df = stream.map(record => (record.key, record.value))
    df.foreachRDD(rdd =>
      println(rdd.take(100).mkString("\n"))
    )

    val s = stream.start()





  }

  @Test
  def testStructureStreaming: Unit = {
    import SparkEnv.spark.implicits._


    val ds = SparkEnv.spark.readStream
      .format("kafka")
//        .format(classOf[KafkaSource].getCanonicalName)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
        .option("startingOffsets", "earliest")
      .load()

      val ds1 = ds.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val q = ds1.writeStream.format("console").outputMode("append").start()
    q.processAllAvailable()

  }

  @Test
  def testStructureStreamingCustomSink: Unit = {
    import SparkEnv.spark.implicits._


    val ds = SparkEnv.spark.readStream
      .format("kafka")
      //        .format(classOf[KafkaSource].getCanonicalName)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()

    val q = ds.writeStream.format(classOf[CustomSinkProvider].getCanonicalName).outputMode("append").start()
    q.processAllAvailable()

  }

  @Test
  def testStructureStreamingCustomSinkTransform: Unit = {
    import SparkEnv.spark.implicits._



    val decompose = udf((msg: String) => {
      val items = msg.split(",")
      Message(new Date(items(0).toLong), items(1).toLong, items(2), items(3))
    }
    )

    val ds = SparkEnv.spark.readStream
      .format("kafka")
      //        .format(classOf[KafkaSource].getCanonicalName)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()
      .withColumn("msg", decompose(col("value").cast(StringType)))
      .select(
        col("key").cast(StringType),
        col("msg.time").alias("time"),
        col("msg.index").alias("index"),
        col("msg.domain").alias("domain"),
        col("msg.ip").alias("ip"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
      )


    val q = ds.writeStream.format(classOf[CustomSinkProvider].getCanonicalName).outputMode("append").start()
    q.processAllAvailable()

  }

}
