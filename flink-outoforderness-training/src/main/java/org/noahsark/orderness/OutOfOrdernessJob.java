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

package org.noahsark.orderness;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.noahsark.orderness.beans.SensorReading;

import java.time.Duration;

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
public class OutOfOrdernessJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 Watermark 生成间隔为 200MS
        env.getConfig().setAutoWatermarkInterval(200L);

        // 设置并行度为 1
        env.setParallelism(1);

        // 用parameter tool工具从程序启动参数中提取配置项，如 --host 192.168.1.1 --port 9000
        // 使用 nc -lk 9000 监听请求并发送数据
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 获取 socket 文本流
        final DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);

        // 转换数据格式
        final SingleOutputStreamOperator<SensorReading> dataStream = dataStreamSource.map(line -> {
            String[] fields = line.split(",");

            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 定义 watermarkStrategy
        final WatermarkStrategy<SensorReading> watermarkStrategy = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp() * 1000);

        final SingleOutputStreamOperator<SensorReading> eventDataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);


        // 分组，开窗及处理迟到数据
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };
        final WindowedStream<SensorReading, String, TimeWindow> windowStream = eventDataStream.keyBy(sensorReading -> sensorReading.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag);

        // 使用 ReduceFunction + ProcessWindowFunction 求最小值
        SingleOutputStreamOperator<Tuple5<String, Long, Long, Long, SensorReading>> reduceProcessWindow = windowStream.reduce(
                new MyReduceFunction(), new MyProcessWindowFunction());

        // 输出 ReduceFunction + ProcessWindowFunction 最小值，输出格式为<key,winStart,winEnd,minValue>
        reduceProcessWindow.print("minTemp-reduce-process");
        reduceProcessWindow.getSideOutput(outputTag).print("late");

        // Execute program, beginning computation.
        env.execute("Flink OutOfOrderness Job");
    }

    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<SensorReading, Tuple5<String, Long, Long, Long, SensorReading>, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<SensorReading> minReadings,
                            Collector<Tuple5<String, Long, Long, Long, SensorReading>> out) {
            SensorReading min = minReadings.iterator().next();
            out.collect(new Tuple5<>(key, context.window().getStart(), context.window().getEnd(), context.currentWatermark(), min));
        }
    }

    private static class MyReduceFunction implements ReduceFunction<SensorReading> {

        @Override
        public SensorReading reduce(SensorReading v1, SensorReading v2) throws Exception {
            final SensorReading sensorReading = v1.getTemperature().compareTo(v2.getTemperature()) > 0 ? v2 : v1;
            return sensorReading;
        }
    }
}
