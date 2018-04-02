package com.mm.bi.utils

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by kequan on 3/27/18.
  */
object SparkSessionUtil {

  /**
    * set SparkConf param
    */
  def getOrCreate(): SparkSession = {

    val properties = ConfigurationUtil.getEnvProperty("env.conf")

    if (properties.equals("conf.properties")) {
      SparkSession
        .builder()
        .appName(this.getClass.getName.replace("$", ""))
        .config("spark.default.parallelism", "10")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.shuffle.consolidateFiles", "true")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.hadoop." + FileInputFormat.PATHFILTER_CLASS, classOf[TempFileInputFilterUtil].getName)
        .master("local[2]")
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .appName(this.getClass.getName.replace("$", ""))
        .config("spark.default.parallelism", "1000")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.shuffle.consolidateFiles", "true")
        .config("spark.sql.shuffle.partitions", "1000")
        .config("spark.hadoop." + FileInputFormat.PATHFILTER_CLASS, classOf[TempFileInputFilterUtil].getName)
        .getOrCreate()
    }
  }

}
