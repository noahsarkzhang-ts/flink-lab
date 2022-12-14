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

package org.noahsark.top;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.noahsark.top.beans.AvgValue;
import org.noahsark.top.beans.SensorReading;
import org.noahsark.top.beans.TopSensor;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
public class TopNStreamJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ?????? Watermark ??????????????? 200MS
        env.getConfig().setAutoWatermarkInterval(200L);

        // ?????????????????? 1
        env.setParallelism(1);

        // ???parameter tool??????????????????????????????????????????????????? --host 192.168.1.1 --port 9000
        // ?????? nc -lk 9000 ???????????????????????????
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // ?????? socket ?????????
        final DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);

        // ??????????????????
        final SingleOutputStreamOperator<SensorReading> dataStream = dataStreamSource.map(line -> {
            String[] fields = line.split(",");

            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // ?????? watermarkStrategy
        final WatermarkStrategy<SensorReading> watermarkStrategy = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp() * 1000);
        final SingleOutputStreamOperator<SensorReading> eventDataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);


        // ????????????????????????????????????
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };
        final WindowedStream<SensorReading, String, TimeWindow> windowStream = eventDataStream.keyBy(sensorReading -> sensorReading.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag);

        // ?????? AggrgationFunction + ProcessWindowFunction ????????????
        SingleOutputStreamOperator<TopSensor> avgProcessWindow = windowStream.aggregate(new MyAggrgationFunction(), new MyProcessWindowFunction());

        // ?????? windowEnd ????????????????????????????????????????????????????????????????????? topN.
        final SingleOutputStreamOperator<String> topStream = avgProcessWindow.keyBy(topSensor -> topSensor.getWinEnd()).process(new TopSensorKeyedProcessFunction());

        // ??????????????????
        avgProcessWindow.getSideOutput(outputTag).print("late");

        // ??????TOP ??????
        topStream.print("top");

        // Execute program, beginning computation.
        env.execute("Flink TopN Job");
    }

    /**
     * ?????? TopSensor ?????????????????????
     */
    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Double, TopSensor, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Double> avgReadings,
                            Collector<TopSensor> out) {
            Double avgValue = avgReadings.iterator().next();

            out.collect(new TopSensor(key, context.window().getStart(), context.window().getEnd(), avgValue));
        }
    }

    /**
     * AggregateFunction, ?????????????????????
     */
    private static class MyAggrgationFunction implements AggregateFunction<SensorReading, AvgValue, Double> {

        @Override
        public AvgValue createAccumulator() {
            return new AvgValue();
        }

        @Override
        public AvgValue add(SensorReading value, AvgValue accumulator) {

            accumulator.aggregate(value.getTemperature());

            return accumulator;
        }

        @Override
        public Double getResult(AvgValue accumulator) {
            return accumulator.avg();
        }

        @Override
        public AvgValue merge(AvgValue a, AvgValue b) {
            return a.merge(b);
        }
    }

    /**
     * ?????? TopN ???
     * 1. ?????????????????????????????????????????????
     * 2. ???????????????????????????????????????????????????windowEnd + 1, ?????????????????? 1 MS,
     *  ?????????????????????????????????????????????????????????????????????
     */
    private static class TopSensorKeyedProcessFunction extends KeyedProcessFunction<Long, TopSensor, String> {

        // ???????????????????????????????????????
        private ListState<TopSensor> sensorList;

        @Override
        public void open(Configuration parameters) throws Exception {

            sensorList = getRuntimeContext().getListState(new ListStateDescriptor<TopSensor>("sensor-state", TopSensor.class));
        }

        @Override
        public void processElement(TopSensor value, Context ctx, Collector<String> out) throws Exception {
            sensorList.add(value);

            ctx.timerService().registerEventTimeTimer(value.getWinEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            List<TopSensor> topList = new ArrayList<>();

            for (TopSensor sensor : sensorList.get()) {
                topList.add(sensor);
            }

            sensorList.clear();

            topList.sort(new Comparator<TopSensor>() {
                @Override
                public int compare(TopSensor o1, TopSensor o2) {
                    return o2.getAvgVlaue().compareTo(o1.getAvgVlaue());
                }
            });

            // ??????????????????????????? String, ????????????
            StringBuilder result = new StringBuilder();
            result.append("\n====================================\n");
            result.append("??????: ").append(timestamp - 1).append("\n");


            for (int i = 0; i < topList.size() - 1; i++) {

                if (i >= 3) {
                    break;
                }

                TopSensor sensor = topList.get(i);

                result.append("No").append(i).append(":")
                        .append("  sensorId=").append(sensor.getId())
                        .append("  ?????????=").append(sensor.getAvgVlaue())
                        .append("\n");
            }
            result.append("====================================\n\n");

            out.collect(result.toString());
        }
    }
}
