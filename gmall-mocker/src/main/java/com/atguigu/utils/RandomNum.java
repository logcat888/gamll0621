package com.atguigu.utils;

import java.util.Random;

/**
 * @author chenhuiup
 * @create 2020-11-03 19:01
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return fromNum + new Random().nextInt(toNum-fromNum+1);
    }
}

