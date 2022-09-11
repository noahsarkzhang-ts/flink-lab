package org.noahsark.window.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.noahsark.window.beans.SensorReading;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 传感器 Source 类
 *
 * @author zhangxt
 * @date 2022/09/01 17:20
 **/
public class MySensorSource implements SourceFunction<SensorReading> {

    /**
     * 定义一个标识位，用来控制数据的产生
     */
    private boolean running = true;

    // 设置10个传感器的初始温度
    private HashMap<String, Double> sensorTempMap = new HashMap<>();

    public MySensorSource() {

        // 定义一个随机数发生器
        Random random = new Random();

        for (int i = 0; i < 10; i++) {
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }

    }

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {

        Random random = new Random();

        while (running) {

            sensorTempMap.entrySet().stream().forEach(entry -> {
                String sensorId = entry.getKey();

                // 在当前温度基础上随机波动
                Double newTemp = entry.getValue() + random.nextGaussian();
                sensorTempMap.put(sensorId, newTemp);

                sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(),newTemp));


            });

            // 睡眠 1S
            TimeUnit.SECONDS.sleep(1);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
