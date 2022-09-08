/*
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

package org.noahsark.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.noahsark.window.beans.SensorReading;
import org.noahsark.window.common.MySensorSource;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class FlinkWindowTrainingJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间语义
        env.getConfig().setAutoWatermarkInterval(200L);
        env.setParallelism(1);

        // 1. 设置 Source
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        // 2. 定义 WatermarkStrategy
        WatermarkStrategy<SensorReading> watermarkStrategy = WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        final SingleOutputStreamOperator<SensorReading> eventDataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);

        // 3. 分组
        final SingleOutputStreamOperator<SensorReading> minTempStream = eventDataStream.keyBy(sensorReading -> sensorReading.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading v1, SensorReading v2) throws Exception {
                        final SensorReading sensorReading = v1.getTemperature().compareTo(v2.getTemperature()) > 0 ? v2 : v1;
                        return sensorReading;
                    }
                });

        // 输出结果
        minTempStream.print("minTemp");
        //dataStream.print("minTemp");

        // Execute program, beginning computation.
        env.execute("Flink Window Training");
    }
}
