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
        // ??????????????????????????????????????????
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

    // ???????????????????????????????????????????????????????????????????????????
    // ??????ID?????? key???
    public static class PseudoWindow extends
            KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

        private final long durationMsec;

        // ?????????????????????????????? Keyed state ??????????????????????????????????????????????????? keyed ?????????
        // ?????????????????????????????????MapState?????????
        private transient MapState<Long, Float> sumOfTips;

        public PseudoWindow(Time duration) {
            this.durationMsec = duration.toMilliseconds();
        }

        @Override
        // ?????????????????????????????????
        public void open(Configuration conf) {
            MapStateDescriptor<Long, Float> sumDesc =
                    new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
            sumOfTips = getRuntimeContext().getMapState(sumDesc);
        }

        @Override
        // ?????????????????????TaxiFare-Event??????????????????????????????????????????????????????????????????
        public void processElement(
                TaxiFare fare,
                Context ctx,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            long eventTime = fare.getEventTimeMillis();
            TimerService timerService = ctx.timerService();

            if (eventTime <= timerService.currentWatermark()) {
                // ????????????????????????????????????????????????
            } else {
                // ??? eventTime ?????????????????????????????????????????????????????????????????????????????????
                long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

                // ?????????????????????????????????
                timerService.registerEventTimeTimer(endOfWindow);

                // ??????????????????????????????????????????????????????
                Float sum = sumOfTips.get(endOfWindow);
                if (sum == null) {
                    sum = 0.0F;
                }
                sum += fare.tip;
                sumOfTips.put(endOfWindow, sum);
            }
        }

        @Override
        // ??????????????????watermark???????????????????????????????????????????????????
        public void onTimer(long timestamp,
                            OnTimerContext context,
                            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

            long driverId = context.getCurrentKey();
            // ????????????????????????????????????
            Float sumOfTips = this.sumOfTips.get(timestamp);

            Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
            out.collect(result);
            this.sumOfTips.remove(timestamp);
        }
    }
}
