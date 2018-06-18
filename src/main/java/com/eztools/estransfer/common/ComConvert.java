package com.eztools.estransfer.common;

/**
 * Created by lcy on 2018/1/20.
 */
public class ComConvert {
    /**
     * object转换int的函数
     */
    public static int toInteger(Object value, int defaultValue) {
        try {
            return (null != value) ? Integer.parseInt(value.toString()) : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * object转换long的函数
     */
    public static long toLong(Object value, long defaultValue) {
        try {
            return (null != value) ? Long.parseLong(value.toString()) : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * object转换double的函数
     */
    public static double toDouble(Object value, double defaultValue) {
        try {
            return (null != value) ? Double.parseDouble(value.toString()) : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * object转换float的函数
     */
    public static float toFloat(Object value, float defaultValue) {
        try {
            return (null != value) ? Float.parseFloat(value.toString()) : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * object转换boolean的函数
     */
    public static boolean toBoolean(Object value, boolean defaultValue) {
        try {
            return (null != value) ? Boolean.parseBoolean(value.toString()) : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * object转换string的函数
     */
    public static String toString(Object value) {
        return (null != value) ? value.toString() : "";
    }
}
