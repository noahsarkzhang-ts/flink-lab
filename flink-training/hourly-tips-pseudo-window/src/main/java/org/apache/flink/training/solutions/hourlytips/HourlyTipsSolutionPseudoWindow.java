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

package org.apache.flink.training.solutions.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsSolutionPseudoWindow {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /**
     * Creates a job using the source and sink provided.
     */
    public HourlyTipsSolutionPseudoWindow(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsSolutionPseudoWindow job =
                new HourlyTipsSolutionPseudoWindow(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator and arrange for watermarking
        DataStream<TaxiFare> fares =
                env.addSource(source)
                        .assignTimestampsAndWatermarks(
                                // taxi fares are in order
                                WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (fare, t) -> fare.getEventTimeMillis()));

        // compute tips per hour for each driver
        // 计算每个司机每小时的小费总和
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .process(new PseudoWindow(Time.hours(1)));

        // find the driver with the highest sum of tips for each hour
        DataStream<Tuple3<Long, Long, Float>> hourlyMax =
                hourlyTips.windowAll(TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2);

        /* You should explore how this alternative (commented out below) behaves.
         * In what ways is the same as, and different from, the solution above (using a windowAll)?
         */

        // DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips.keyBy(t -> t.f0).maxBy(2);

        hourlyMax.addSink(sink);

        // execute the transformation pipeline
        return env.execute("Hourly Tips");
    }

    // 在时长跨度为一小时的窗口中计算每个司机的小费总和。
    // 司机ID作为 key。
    public static class PseudoWindow extends
            KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

        private final long durationMsec;

        // 每个窗口都持有托管的 Keyed state 的入口，并且根据窗口的结束时间执行 keyed 策略。
        // 每个司机都有一个单独的MapState对象。
        private transient MapState<Long, Float> sumOfTips;

        public PseudoWindow(Time duration) {
            this.durationMsec = duration.toMilliseconds();
        }

        @Override
        // 在初始化期间调用一次。
        public void open(Configuration conf) {
            MapStateDescriptor<Long, Float> sumDesc =
                    new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
            sumOfTips = getRuntimeContext().getMapState(sumDesc);
        }

        @Override
        // 每个票价事件（TaxiFare-Event）输入（到达）时调用，以处理输入的票价事件。
        public void processElement(
                TaxiFare fare,
                Context ctx,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            long eventTime = fare.getEventTimeMillis();
            TimerService timerService = ctx.timerService();

            if (eventTime <= timerService.currentWatermark()) {
                // 事件延迟；其对应的窗口已经触发。
            } else {
                // 将 eventTime 向上取值并将结果赋值到包含当前事件的窗口的末尾时间点。
                long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

                // 在窗口完成时将启用回调
                timerService.registerEventTimeTimer(endOfWindow);

                // 将此票价的小费添加到该窗口的总计中。
                Float sum = sumOfTips.get(endOfWindow);
                if (sum == null) {
                    sum = 0.0F;
                }
                sum += fare.tip;
                sumOfTips.put(endOfWindow, sum);
            }
        }

        @Override
        // 当当前水印（watermark）表明窗口现在需要完成的时候调用。
        public void onTimer(long timestamp,
                            OnTimerContext context,
                            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

            long driverId = context.getCurrentKey();
            // 查找刚结束的一小时结果。
            Float sumOfTips = this.sumOfTips.get(timestamp);

            Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
            out.collect(result);
            this.sumOfTips.remove(timestamp);
        }
    }
}
