package com.mm.bi.utils

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkConf

/**
  * SparkConf Util
  *
  * Created by kequan on 3/25/18.
  */
object SparkConfUtil {

  /**
    * set SparkConf param
    *
    * @param conf
    */
  def setConf(conf: SparkConf) {

    val properties = ConfigurationUtil.getEnvProperty("env.conf")

    if (properties.equals("conf.properties")) {
      conf.setMaster("local[2]")
        .set("spark.default.parallelism", "10")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.consolidateFiles", "true")
        .set("spark.sql.shuffle.partitions", "10")
        .set("spark.hadoop." + FileInputFormat.PATHFILTER_CLASS, classOf[TempFileInputFilterUtil].getName)
    } else if (properties.equals("conf_product.properties")) {
      conf.set("spark.default.parallelism", "500")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.consolidateFiles", "true")
        .set("spark.sql.shuffle.partitions", "500")
        .set("spark.hadoop." + FileInputFormat.PATHFILTER_CLASS, classOf[TempFileInputFilterUtil].getName)
    }

  }

}
