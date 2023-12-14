package com.lxf.model;

public class Item {
    private String itemId;
    private String orderId;
    private Long i_time;
    private String suk;
    private Integer amnout;
    private Double price;

    public String getItemId() {
        return itemId;
    }

    public String getOrderId() {
        return orderId;
    }

    public Long getI_time() {
        return i_time;
    }

    public String getSuk() {
        return suk;
    }

    public Integer getAmnout() {
        return amnout;
    }

    public Double getPrice() {
        return price;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setI_time(Long i_time) {
        this.i_time = i_time;
    }

    public void setSuk(String suk) {
        this.suk = suk;
    }

    public void setAmnout(Integer amnout) {
        this.amnout = amnout;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Item{" +
                "itemId='" + itemId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", i_time=" + i_time +
                ", suk='" + suk + '\'' +
                ", amnout=" + amnout +
                ", price=" + price +
                '}';
    }
}
