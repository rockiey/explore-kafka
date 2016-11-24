package com.rockiey.kafka

import java.util.concurrent.{ExecutorService, Executors}
import java.util.{Date, Properties, Random}

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.junit.{After, Before, Test}

class TestConsumer {
  val zookeeper = "localhost:2181"
  val groupId = "group2"
  val topic = "test"
  val config = createConsumerConfig(zookeeper, groupId)
  val consumer = Consumer.create(config)
  var executor: ExecutorService = null


  def shutdown() = {
    if (consumer != null)
      consumer.shutdown()
  }

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)

    props.put("auto.offset.reset", "smallest")
//    props.put("auto.offset.reset", "largest")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    val config = new ConsumerConfig(props)
    config
  }

  @Test
  def test: Unit = {
    consume(2)

    Thread.sleep(5000)
    println("finished")
  }

  def consume(numThreads: Int) = {
    val topicCountMap = Map(topic -> numThreads)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get

    executor = Executors.newFixedThreadPool(numThreads);
    var threadNumber = 0;
    for (stream <- streams) {
      executor.submit(new ConsumerRunner(stream, threadNumber))
      threadNumber += 1
    }
  }
}

