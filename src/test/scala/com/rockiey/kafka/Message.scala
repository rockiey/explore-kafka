package com.rockiey.kafka

import java.sql.Date

case class Message(time: Date, index: Long, domain: String, ip: String)
