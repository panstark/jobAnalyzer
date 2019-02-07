package com.sea.spark.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

    public static String getNowDate(){

        String nowDate =  new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        return nowDate;
    }

}
