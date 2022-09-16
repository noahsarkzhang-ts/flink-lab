package org.noahsark.join.beans;

/**
 * 打车信息及费用
 *
 * @author zhangxt
 * @date 2022/09/14 17:32
 **/
public class RideFare {

    private TaxiRide ride;

    private TaxiFare fare;

    public RideFare() {
    }

    public RideFare(TaxiRide ride, TaxiFare fare) {
        this.ride = ride;
        this.fare = fare;
    }

    public TaxiRide getRide() {
        return ride;
    }

    public void setRide(TaxiRide ride) {
        this.ride = ride;
    }

    public TaxiFare getFare() {
        return fare;
    }

    public void setFare(TaxiFare fare) {
        this.fare = fare;
    }

    @Override
    public String toString() {
        return "RideFare{" +
                "ride=" + ride +
                ", fare=" + fare +
                '}';
    }
}
