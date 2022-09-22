package org.noahsark.top.beans;

/**
 * Sensor Window 平均值
 *
 * @author zhangxt
 * @date 2022/09/21 10:29
 **/
public class TopSensor {

    // Sensor id
    private String id;

    // 窗口开始
    private Long winStart;

    // 窗口结束值
    private Long winEnd;

    // 平均值
    private Double avgVlaue;

    public TopSensor() {
    }

    public TopSensor(String id, Long winStart, Long winEnd, Double avgVlaue) {
        this.id = id;
        this.winStart = winStart;
        this.winEnd = winEnd;
        this.avgVlaue = avgVlaue;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getWinStart() {
        return winStart;
    }

    public void setWinStart(Long winStart) {
        this.winStart = winStart;
    }

    public Long getWinEnd() {
        return winEnd;
    }

    public void setWinEnd(Long winEnd) {
        this.winEnd = winEnd;
    }

    public Double getAvgVlaue() {
        return avgVlaue;
    }

    public void setAvgVlaue(Double avgVlaue) {
        this.avgVlaue = avgVlaue;
    }

    @Override
    public String toString() {
        return "TopSensor{" +
                "id='" + id + '\'' +
                ", winStart=" + winStart +
                ", winEnd=" + winEnd +
                ", avgVlaue=" + avgVlaue +
                '}';
    }
}
