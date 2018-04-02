package com.mm.bi.utils

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.DataFrame

/**
  * file util
  * Created by kequan on 3/25/18.
  */
object FileUtil {

  /**
    * delete file
    *
    * @param fileName
    */
  def delfile(fileName: String) = {
    val file = new File(fileName)
    if (file.exists()) {
      file.delete()
    }
  }

  /**
    * overWrite string To file
    *
    * @param fileName
    * @param s
    */
  def overWriteTofile(fileName: String, s: String) = {
    //将数据存入文件
    val file = new File(fileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    val writer = new BufferedWriter(new FileWriter(file, true))
    writer.write(s, 0, s.length)
    writer.flush()
    writer.close()
  }


  /**
    * apppend string To file
    *
    * @param fileName
    * @param s
    */
  def apppendTofile(fileName: String, s: String) = {
    val file = new File(fileName)
    if (!file.exists()) {
      file.createNewFile()
    }
    val writer = new BufferedWriter(new FileWriter(file, true))
    writer.append(s)
    writer.newLine()
    writer.flush()
    writer.close()
  }

  /**
    * append df to file
    *
    * @param df
    * @param fileName
    */
  def apppendDfTofile(fileName: String, df: DataFrame) = {
    df.foreachPartition(iter => {
      iter.foreach(t => {
        apppendTofile(fileName, t.toString());
      })
    })
  }

}
