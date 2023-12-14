package com.lxf.model;

public class Order {
    private  String orderId;
    private  Long time;
    private  Double money;

    public String getOrderId() {
        return orderId;
    }

    public Long getTime() {
        return time;
    }

    public Double getMoney() {
        return money;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", time=" + time +
                ", money=" + money +
                '}';
    }
}
