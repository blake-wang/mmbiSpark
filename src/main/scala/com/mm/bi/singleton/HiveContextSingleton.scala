package com.mm.bi.singleton

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * HiveContext Singleton mode
  * Created by kequan on 3/25/18.
  */
object HiveContextSingleton {
  @transient private var instance: HiveContext = _

  def getInstance(sparkContext: SparkContext): HiveContext = {
    if (instance == null) {
      instance = new HiveContext(sparkContext)
    }
    instance
  }
}
