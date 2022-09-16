package org.noahsark.join.beans;

/**
 * Taxi 费用
 *
 * @author zhangxt
 * @date 2022/09/14 17:28
 **/
public class TaxiFare {

    // 表示一次打车事件
    private String rideId;

    // 打车费用
    private Double fare;

    // 时间戳
    private Long timestamp;

    public TaxiFare() {
    }

    public TaxiFare(String rideId, Double fare, Long timestamp) {
        this.rideId = rideId;
        this.fare = fare;
        this.timestamp = timestamp;
    }

    public String getRideId() {
        return rideId;
    }

    public void setRideId(String rideId) {
        this.rideId = rideId;
    }

    public Double getFare() {
        return fare;
    }

    public void setFare(Double fare) {
        this.fare = fare;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TaxiFare{" +
                "rideId='" + rideId + '\'' +
                ", fare=" + fare +
                ", timestamp=" + timestamp +
                '}';
    }
}
