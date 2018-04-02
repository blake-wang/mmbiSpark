package com.mm.bi.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.collection.mutable.ArrayBuffer

/**
  * MySQL database util
  *
  * Created by kequan on 3/25/18.
  */
object JdbcUtil {

  /**
    * get mysql  connection
    *
    * @return Mysql Connection
    */
  def getConn(): Connection = {
    val url = ConfigurationUtil.getProperty("jdbc.url");
    val driver = ConfigurationUtil.getProperty("jdbc.driver");
    val user = ConfigurationUtil.getProperty("jdbc.user");
    val pwd = ConfigurationUtil.getProperty("jdbc.pwd");
    try {
      Class.forName(driver)
      return DriverManager.getConnection(url, user, pwd)
    } catch {
      case ex: Exception => {
        println("Get mysql connection Exception：Exception=" + ex)
      }
        return null
    }
  }

  /**
    * Mysql batch update data in each partition of spark
    *
    * @param sqlText
    * @param params
    */
  def executeBatch(sqlText: String, params: ArrayBuffer[ArrayBuffer[Any]]): Unit = {
    if (params.length > 0) {
      var conn: Connection = getConn();
      var pstat: PreparedStatement = conn.prepareStatement(sqlText);
      pstat.clearBatch()
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstat.setObject(index + 1, param(index))
        }
        pstat.addBatch()
      }
      pstat.executeBatch
      pstat.close();
      conn.close();
      params.clear();
    }
  }

  /**
    * Mysql update a data
    *
    * @param pstat
    * @param params
    * @param conn
    */
  def executeUpdate(pstat: PreparedStatement, params: ArrayBuffer[ArrayBuffer[Any]], conn: Connection): Unit = {
    if (params.length > 0) {
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstat.setObject(index + 1, param(index))
        }
        pstat.executeUpdate()
      }
      params.clear();
    }
  }
}
