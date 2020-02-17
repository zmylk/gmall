package com.atguigu.gmall.gmallpublisher.bean;

public class Option {

    //名字
    String name;
    //值
    Double value;

    public Option(String name, Double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}


