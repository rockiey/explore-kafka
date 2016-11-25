package com.rockiey.kafka

import kafka.consumer.KafkaStream

class ConsumerRunner(stream: KafkaStream[Array[Byte], Array[Byte]],
                     val threadNumber: Int) extends Runnable {
  override def run(): Unit = {
    val it = stream.iterator()
    println(s"Thread $threadNumber started")

    while (it.hasNext()) {
      val msg = new String(it.next().message())
      println(System.currentTimeMillis() + ",Thread " + threadNumber + ": " + msg)
    }

    println(s"Thread $threadNumber stopped")
  }
}
