package org.noahsark.top.beans;

/**
 * 平均值
 *
 * @author zhangxt
 * @date 2022/09/19 20:53
 **/
public class AvgValue {

    // 总和
    private double sum;

    // 计数
    private int count;

    public AvgValue() {
        sum = 0.0D;
        count = 0;
    }

    public AvgValue(double sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public void aggregate(double value) {
        count++;
        sum += value;
    }

    public double avg() {

        return sum / count;
    }

    public AvgValue merge(AvgValue other) {

        this.sum += other.getSum();
        this.count += other.getCount();

        return this;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AvgValue{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }
}
