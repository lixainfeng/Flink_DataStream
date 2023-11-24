package com.lxf.test;

import java.util.Random;

public class test2 {
    public static void main(String[] args) {
        String[] arrays={"牛肉面","家味道"};
        Random randoms = new Random();
        int i = randoms.nextInt(1);
        System.out.println(arrays[i]);

    }
}
