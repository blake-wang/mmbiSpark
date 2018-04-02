package com.mm.bi.singleton

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * SQLContext Singleton mode
  * Created by kequan on 3/25/18.
  */
object SQLContextSingleton {
  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
