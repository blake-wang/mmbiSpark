package com.mm.bi.utils

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * spark sql util
  *
  * Created by kequan on 3/25/18.
  */
object SparkSQLUtil {

  /**
    * insert df Batch into mysql
    *
    * @param df
    */
  def foreachPartitionBatch(df: DataFrame, sql: String) = {
    df.foreachPartition(iter => {
      if (!iter.isEmpty) {
        var params_data = new ArrayBuffer[Any]
        var params_row = new ArrayBuffer[ArrayBuffer[Any]]()

        iter.foreach(row => {

          params_data.clear()

          for (x <- 0 to row.length - 1) {
            params_data.+=(row.get(x))
          }
          params_row.+=(params_data)

        })
        JdbcUtil.executeBatch(sql, params_row)
      }

    })
  }

  /**
    * insert df  into mysql
    *
    * @param df
    */
  def foreachPartition(df: DataFrame, sql: String) = {
    df.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn: Connection = JdbcUtil.getConn();
        var pst: PreparedStatement = conn.prepareStatement(sql);
        var params_data = new ArrayBuffer[Any]
        var params_row = new ArrayBuffer[ArrayBuffer[Any]]()
        iter.foreach(row => {
          try {
            params_data.clear()
            for (x <- 0 to row.length - 1) {
              params_data.+=(row.get(x))
            }
            params_row.+=(params_data)
            JdbcUtil.executeUpdate(pst, params_row, conn)
          } catch {
            case ex: Exception => {
              println("mysql executeUpdate Exception：data==" + params_data.toString())
              println("mysql executeUpdate Exception：Exception==" + ex)
            }
          }

        })
        pst.close()
        conn.close()
      }
    })
  }


}
