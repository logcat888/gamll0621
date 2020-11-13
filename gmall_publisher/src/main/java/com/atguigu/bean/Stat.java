package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

/**
 * @author chenhuiup
 * @create 2020-11-11 21:29
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Stat {
    private ArrayList<Options> options;
    private String title;
}
