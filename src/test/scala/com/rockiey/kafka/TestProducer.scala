package com.knockdata.redback.kafka

import java.util.{Date, Properties, Random}

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import kafka.producer.{KeyedMessage, Producer}
import org.junit.{After, Before, Test}


class TestProducer {

  val brokers = "localhost:9092"
  val topic = "test"

  val rnd = new Random()
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
  props.put("producer.type", "async")
  //props.put("request.required.acks", "1")

  val producer = new KafkaProducer[String, String](props)

  @Before
  def before: Unit = {

//    val config = new ProducerConfig(props)
//    producer = new KafkaProducer[String, String](config)
  }

  @After
  def after: Unit = {
    producer.close()
  }

  def produce(events: Int): Unit = {
    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String](topic, ip, msg)
      producer.send(data)
    }

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  }

  @Test
  def testProducer: Unit = {
    produce(100)
  }

  @Test
  def testConsumer {

  }
}
