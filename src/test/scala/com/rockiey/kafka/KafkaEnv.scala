package com.rockiey.kafka

import sys.process._

object KafkaEnv {
  def start: Unit = {
    val startCmd = "docker run -d -p 2181:2181 -p 9092:9092 --name kafka kafka"

    startCmd !

  }

  def stop: Unit = {

  }
}
