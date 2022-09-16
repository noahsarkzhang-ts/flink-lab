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

package org.noahsark.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.noahsark.join.beans.RideFare;
import org.noahsark.join.beans.TaxiFare;
import org.noahsark.join.beans.TaxiRide;

import java.time.Duration;
import java.util.logging.Logger;

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
public class JoinDataStreamJob {

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
        String host1 = parameterTool.get("host1");
        int port1 = parameterTool.getInt("port1");

        // 获取 socket 文本流
        final DataStreamSource<String> rideSource = env.socketTextStream(host1, port1);

        // 转换数据格式
        final SingleOutputStreamOperator<TaxiRide> rideDataStream = rideSource.map(line -> {
            String[] fields = line.split(",");

            return new TaxiRide(fields[0], fields[1], Long.parseLong(fields[2]));
        });

        // 定义 watermarkStrategy
        final WatermarkStrategy<TaxiRide> riderWatermarkStrategy = WatermarkStrategy
                .<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp() * 1000)
                .withIdleness(Duration.ofSeconds(1));

        final SingleOutputStreamOperator<TaxiRide> rideEventDataStream = rideDataStream.assignTimestampsAndWatermarks(riderWatermarkStrategy);


        String host2 = parameterTool.get("host2");
        int port2 = parameterTool.getInt("port2");

        // 获取 socket 文本流
        final DataStreamSource<String> fareSource = env.socketTextStream(host2, port2);

        // 转换数据格式
        final SingleOutputStreamOperator<TaxiFare> fareDataStream = fareSource.map(line -> {
            String[] fields = line.split(",");

            return new TaxiFare(fields[0], Double.parseDouble(fields[1]), Long.parseLong(fields[2]));
        });

        // 定义 watermarkStrategy
        final WatermarkStrategy<TaxiFare> fareWatermarkStrategy = WatermarkStrategy
                .<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp() * 1000)
                .withIdleness(Duration.ofSeconds(1));

        final SingleOutputStreamOperator<TaxiFare> fareEventDataStream = fareDataStream.assignTimestampsAndWatermarks(fareWatermarkStrategy);

        final KeyedStream<TaxiRide, String> taxiRideStringKeyedStream = rideEventDataStream.keyBy(taxiRide -> taxiRide.getRideId());
        final KeyedStream<TaxiFare, String> taxiFareStringKeyedStream = fareEventDataStream.keyBy(taxiFare -> taxiFare.getRideId());

        final SingleOutputStreamOperator<RideFare> rideFareStream = taxiRideStringKeyedStream.connect(taxiFareStringKeyedStream)
                .process(new RiderFareProcessMapFuntion());
        rideFareStream.print("ride-fare");

        OutputTag<RideFare> outputTag = new OutputTag<RideFare>("ride-fare") {
        };
        final DataStream<RideFare> sideOutputStream = rideFareStream.getSideOutput(outputTag);
        sideOutputStream.print("side-output");


        // Execute program, beginning computation.
        env.execute("Flink Join Training Job");
    }

    private static class RiderFareProcessMapFuntion extends KeyedCoProcessFunction<String, TaxiRide, TaxiFare, RideFare> {

        // 超时时间为 60 S
        private static final int TIMET_OUT_MS = 60 * 1000;

        // 存放 TaxiRide 状态
        private ValueState<TaxiRide> rideState;

        // 存放 TaxiFare 状态
        private ValueState<TaxiFare> fareState;

        // 存放超时时间戳
        private ValueState<Long> timeoutState;

        @Override
        public void open(Configuration parameters) throws Exception {
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiRide>("ride", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<TaxiFare>("fare", TaxiFare.class));
            timeoutState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeout", Long.class));
        }

        @Override
        public void processElement1(TaxiRide value, Context ctx, Collector<RideFare> out) throws Exception {
            TaxiFare fare = fareState.value();
            if (fare != null) {
                fareState.clear();
                out.collect(new RideFare(value, fare));

                Long timeout = timeoutState.value();

                ctx.timerService().deleteEventTimeTimer(timeout);
                System.out.println("del Timer:" + timeout);

                timeoutState.clear();

            } else {
                rideState.update(value);

                Long timeout = ctx.timestamp() + TIMET_OUT_MS;
                timeoutState.update(timeout);

                ctx.timerService().registerEventTimeTimer(timeout);
                System.out.println("reg Timer:" + timeout);
            }

        }

        @Override
        public void processElement2(TaxiFare value, Context ctx, Collector<RideFare> out) throws Exception {
            TaxiRide ride = rideState.value();
            if (ride != null) {
                rideState.clear();
                out.collect(new RideFare(ride, value));

                Long timeout = timeoutState.value();

                ctx.timerService().deleteEventTimeTimer(timeout);
                System.out.println("del Timer:" + timeout);

                timeoutState.clear();

            } else {
                fareState.update(value);

                Long timeout = ctx.timestamp() + TIMET_OUT_MS;
                timeoutState.update(timeout);

                ctx.timerService().registerEventTimeTimer(timeout);
                System.out.println("reg Timer:" + timeout);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<RideFare> out) throws Exception {

            System.out.println("onTimer");

            TaxiRide ride = rideState.value();
            TaxiFare fare = fareState.value();

            RideFare rideFare = new RideFare();
            rideFare.setFare(fare);
            rideFare.setRide(ride);

            OutputTag<RideFare> outputTag = new OutputTag<RideFare>("ride-fare") {
            };
            ctx.output(outputTag, rideFare);

            rideState.clear();
            fareState.clear();
            timeoutState.clear();
        }
    }
}
