package org.noahsark.join.beans;

/**
 * Taxi 事件
 *
 * @author zhangxt
 * @date 2022/09/14 17:25
 **/
public class TaxiRide {

    // 表示一次打车
    private String rideId;

    // 事件类型：start or end
    private String type;

    private Long timestamp;

    public TaxiRide() {
    }

    public TaxiRide(String rideId, String type, Long timestamp) {
        this.rideId = rideId;
        this.type = type;
        this.timestamp = timestamp;
    }

    public String getRideId() {
        return rideId;
    }

    public void setRideId(String rideId) {
        this.rideId = rideId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TaxiRide{" +
                "rideId=" + rideId +
                ", type='" + type + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
