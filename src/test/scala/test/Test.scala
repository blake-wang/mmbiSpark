package test

import com.mm.bi.utils.JdbcUtil

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 3/25/18.
  */
object Test {
  def main(args: Array[String]): Unit = {
    var params_data = new ArrayBuffer[Any]

    params_data.+=("aaa")
    params_data.+=("bbb")

    println(params_data.toString())
  }
}
