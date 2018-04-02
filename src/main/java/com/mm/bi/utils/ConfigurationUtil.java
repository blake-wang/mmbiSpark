package com.mm.bi.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * load configuration file util
 * <p>
 * Created by kequan on 3/25/18.
 */
public class ConfigurationUtil {

    /**
     * According to the configuration file key, get value
     *
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        Properties properties = new Properties();
        InputStream in = ConfigurationUtil.class.getClassLoader().getResourceAsStream(getEnvProperty("env.conf"));
        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (String) properties.get(key);
    }

    /**
     * According to the env.properties, get configuration file
     *
     * @param key
     * @return
     */
    public static String getEnvProperty(String key) {
        Properties properties = new Properties();
        InputStream in = ConfigurationUtil.class.getClassLoader().getResourceAsStream("env.properties");
        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (String) properties.get(key);
    }

    /**
     * According to the configuration file key, get int Value
     *
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * According to the configuration file key, get int Value
     *
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * According to the configuration file key, get Long Value
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
