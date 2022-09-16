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

package org.noahsark.process;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.noahsark.process.beans.CountWithTimestamp;
import org.noahsark.process.beans.UserRequest;

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
public class ProcessFunctionJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为 1
        env.setParallelism(1);

        // 设置 Watermark 生成间隔为 200MS
        env.getConfig().setAutoWatermarkInterval(200L);

        // 用parameter tool工具从程序启动参数中提取配置项，如 --host 192.168.1.1 --port 9000
        // 使用 nc -lk 9000 监听请求并发送数据
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 获取 socket 文本流
        final DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);

        // 转换数据格式
        final SingleOutputStreamOperator<UserRequest> dataStream = dataStreamSource.map(line -> {
            String[] fields = line.split(",");

            return new UserRequest(fields[0], fields[1], Long.parseLong(fields[2]));
        });

        // 定义 watermarkStrategy
        final WatermarkStrategy<UserRequest> watermarkStrategy = WatermarkStrategy
                .<UserRequest>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp() * 1000);
        final SingleOutputStreamOperator<UserRequest> eventDataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);

        final SingleOutputStreamOperator<CountWithTimestamp> processStream = eventDataStream.keyBy(userRequest -> userRequest.getUserId())
                .process(new RequestCountFunction());

        processStream.print("request-count");

        // Execute program, beginning computation.
        env.execute("Flink Process Function Job");
    }

    private static class RequestCountFunction extends KeyedProcessFunction<String, UserRequest, CountWithTimestamp> {

        // 申明状态变量，存储用户请求次数
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义状态变量
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("mystate", CountWithTimestamp.class));
        }

        @Override
        public void processElement(UserRequest userRequest, Context context, Collector<CountWithTimestamp> collector) throws Exception {

            // 获取状态变量
            CountWithTimestamp current = state.value();

            // 如果状态变量为空则初始化
            if (current == null) {
                current = new CountWithTimestamp();
                current.setUserId(userRequest.getUserId());
            }

            // 计数
            current.setCount(current.getCount() + 1);

            // 设置上次的修改时间
            current.setLastModified(context.timestamp());

            // 更新状态变量
            state.update(current);

            // 注册定时器
            context.timerService().registerEventTimeTimer(current.getLastModified() + 60000);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<CountWithTimestamp> out) throws Exception {
            CountWithTimestamp result = state.value();

            // 如果两次操作之间间隔 60S, 则输出
            if (timestamp >= (result.getLastModified() + 60000)) {
                out.collect(result);
            }
        }
    }
}
