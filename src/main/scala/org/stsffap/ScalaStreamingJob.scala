package org.stsffap

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object ScalaStreamingJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

   env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = env.fromElements(Order(1,1), Shipment(1,3), Delivery(1, 5),
      Order(2, 2), Order(3, 6), Shipment(2, 4), Delivery(2, 21))
      //.assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
      .assignAscendingTimestamps( _.timestamp )
      .keyBy("orderId")

    input.getExecutionConfig.setAutoWatermarkInterval(1000L)

    // calculate the processing warnings
    val processingPattern = Pattern.begin[Event]("ordered").subtype(classOf[Order])
      .followedBy("shipped").subtype(classOf[Shipment])
      .within(Time.milliseconds(5))

    val processingPatternStream = CEP.pattern(input, processingPattern)

    val processingResult: DataStream[Either[ProcessingWarning, ProcessingSuccess]] = processingPatternStream.select {
      (partialPattern, timestamp) => ProcessingWarning(partialPattern("ordered").orderId, timestamp)
    } {
      fullPattern => 
        ProcessingSuccess(
          fullPattern("ordered").orderId,
          fullPattern("shipped").timestamp,
          fullPattern("shipped").timestamp - fullPattern("ordered").timestamp)
    }

    // calculate the delivery warnings
    val deliveryPattern = Pattern.begin[Event]("shipped").where(_.status == "Shipped")
      .followedBy("delivered").where(_.status == "Delivered")
      .within(Time.milliseconds(8))


    val deliveryPatternStream = CEP.pattern(input, deliveryPattern)

    val deliveryResult: DataStream[Either[DeliveryWarning, DeliverySuccess]] = deliveryPatternStream.select {
      (partialPattern, tstamp) => DeliveryWarning(partialPattern("shipped").orderId, tstamp)
    } {
      fullPattern =>
        DeliverySuccess(
          fullPattern("shipped").orderId,
          fullPattern("delivered").timestamp,
          fullPattern("delivered").timestamp - fullPattern("shipped").timestamp
        )
    }

    val processingWarnings = processingResult.flatMap (_.left.toOption)

    val processingSuccesses = processingResult.flatMap (_.right.toOption)

    val deliveryWarnings = deliveryResult.flatMap (_.left.toOption)
    val deliverySuccesses = deliveryResult.flatMap (_.right.toOption)

    processingWarnings.writeAsCsv("/tmp/flink-out/processsingWarnings.csv", FileSystem.WriteMode.OVERWRITE)
    processingSuccesses.writeAsCsv("/tmp/flink-out/processsingSuccesses.csv", FileSystem.WriteMode.OVERWRITE)
    deliveryWarnings.writeAsCsv("/tmp/flink-out/deliveryWarnings.csv", FileSystem.WriteMode.OVERWRITE)
    deliverySuccesses.writeAsCsv("/tmp/flink-out/deliverySuccesses.csv", FileSystem.WriteMode.OVERWRITE)

    env.execute("Flink Streaming Scala API Skeleton")
  }
}

/**
  * This generator generates watermarks that are lagging behind processing time by a certain amount.
  * It assumes that elements arrive in Flink after at most a certain time.
  */
class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[Event] {

  val maxTimeLag = 1L; // 5 milliseconds

  override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = {
    element.timestamp
  }

  override def getCurrentWatermark(): Watermark = {
    // return the watermark as current time minus the maximum time lag
    new Watermark(System.currentTimeMillis() - maxTimeLag)
  }
}


abstract class Event(var orderId: Long, var timestamp: Long, var status: String) {
  def this() {
    this(-1L, -1L, "Undefined")
  }
}

class Order(orderId: Long, timestamp: Long) extends Event(orderId, timestamp, "Received") {
  def this() {
    this(-1, -1)
  }
}

object Order {
  def apply(orderId: Long, timestamp: Long) = new Order(orderId, timestamp)
}

class Shipment(orderId: Long, timestamp: Long) extends Event(orderId, timestamp, "Shipped") {
  def this() {
    this(-1, -1)
  }
}

object Shipment {
  def apply(orderId: Long, timestamp: Long) = new Shipment(orderId, timestamp)
}

class Delivery(orderId: Long, timestamp: Long) extends Event(orderId, timestamp, "Delivered")

object Delivery {
  def apply(orderId: Long, timestamp: Long) = new Delivery(orderId, timestamp)
}

case class ProcessingSuccess(orderId: Long, timestamp: Long, duration: Long)

case class ProcessingWarning(orderId: Long, timestamp: Long)

case class DeliverySuccess(orderId: Long, timestamp: Long, duration: Long)

case class DeliveryWarning(orderId: Long, timestamp: Long)



