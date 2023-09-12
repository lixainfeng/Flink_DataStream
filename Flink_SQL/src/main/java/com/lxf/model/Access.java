package com.lxf.model;

public class Access {
    private Long time;
    private String name;
    private Integer conut;


    public Access(Long time, String name, Integer conut) {
        this.time = time;
        this.name = name;
        this.conut = conut;
    }


    public Access() {
    }



    @Override
    public String toString() {
        return "Access{" +
                "time=" + time +
                ", name='" + name + '\'' +
                ", conut=" + conut +
                '}';
    }



    public void setTime(Long time) {
        this.time = time;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setConut(Integer conut) {
        this.conut = conut;
    }



    public Long getTime() {
        return time;
    }

    public String getName() {
        return name;
    }

    public Integer getConut() {
        return conut;
    }



}
