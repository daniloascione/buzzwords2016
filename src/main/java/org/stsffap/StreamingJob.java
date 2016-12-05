package org.stsffap;

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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;


public class StreamingJob {

	enum Status {
		DELIVERED,
		SHIPPED,
		RECEIVED
	}

	public abstract static class ProcessingEvent {
		private final long orderId;
		private final long timestamp;

		public ProcessingEvent(long orderId, long timestamp) {
			this.orderId = orderId;
			this.timestamp = timestamp;
		}

		public long getOrderId() {
			return orderId;
		}

		public long getTimestamp() {
			return timestamp;
		}
	}

	public static class ProcessingSuccess extends ProcessingEvent {

		public ProcessingSuccess(long orderId, long timestamp) {
			super(orderId, timestamp);
		}
	}

	public static class ProcessingWarning extends ProcessingEvent {

		public ProcessingWarning(long orderId, long timestamp) {
			super(orderId, timestamp);
		}
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Pattern<Tuple3<Long, Long, Status>, ?> processingPattern = Pattern.<Tuple3<Long, Long, Status>>begin("received").where(
				(message) -> message.f2.equals(Status.RECEIVED)
		).followedBy("shipped").where(
				(message) -> message.f2.equals(Status.SHIPPED)
		).within(Time.hours(1));

		PatternStream<Tuple3<Long, Long, Status>> patternStream = CEP.pattern(null, processingPattern);

		DataStream<Either<ProcessingWarning, ProcessingSuccess>> result = patternStream.select(
			(pattern, timestamp) -> {
				Tuple3<Long, Long, Status> message = pattern.get("received");

				return new ProcessingWarning(message.f0, timestamp);
			},
			(pattern) -> {
				Tuple3<Long, Long, Status> message = pattern.get("shipped");

				return new ProcessingSuccess(message.f0, message.f1);
			}
		);

		DataStream<ProcessingWarning> warnings = result.flatMap(
				(Either<ProcessingWarning, ProcessingSuccess> processingEvent, Collector<ProcessingWarning> out) -> {
					if (processingEvent.isLeft()) {
						out.collect(processingEvent.left());
					}
				}
		);

		env.execute("Flink Streaming Java API Skeleton");
	}
}
