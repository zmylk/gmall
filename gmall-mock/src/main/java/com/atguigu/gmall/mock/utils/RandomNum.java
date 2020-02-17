package com.atguigu.gmall.mock.utils;

import java.util.Random;

public class RandomNum {

    public static final  int getRandInt(int fromNum,int toNum){
       return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }

    public static final  Double getRandDouble(int fromNum,int toNum){
        return     fromNum+ new Random().nextInt(toNum-fromNum+1)+ 0.0;
    }

    public static void main(String[] args) {
        System.out.println(getRandDouble(1,1000));
    }
}
 
 
