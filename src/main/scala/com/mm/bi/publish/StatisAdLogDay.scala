package com.mm.bi.publish

import com.mm.bi.singleton.HiveContextSingleton
import com.mm.bi.utils.{DateUtil, SparkConfUtil, SparkSQLUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * statis_adlog_day
  * Created by kequan on 3/28/18.
  */
object StatisAdLogDay {

  var curDay = "";

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length == 1) {
      curDay = args(0)
    } else {
      curDay = DateUtil.getOneHourBeforeDate();
    }

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    SparkConfUtil.setConf(sparkConf);

    val sc = new SparkContext(sparkConf);
    val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)

    val sql_hive =
      """
        SELECT 'curDay' as dayTime,
        |   a.GameId,
        |   a.Source,
        |   NVL(IpCount,0) AS IpCount,
        |   NVL(PvCount,0) AS PvCount,
        |   NVL(UvCount,0) AS UvCount,
        |   NVL(Btn1Count,0) AS Btn1Count,
        |   NVL(Btn2Count,0) AS Btn2Count,
        |   NVL(Btn3Count,0) As Btn3Count
        |FROM mmbi.source_info AS a
        |LEFT JOIN (
        |   SELECT b.Source,b.GameId
        |      ,COUNT(DISTINCT a.RefererIp) AS IpCount
        |      ,COUNT(IF(a.LogType = 'Load', 1, NULL)) AS PvCount
        |      ,COUNT(DISTINCT UUID) AS UvCount
        |      ,COUNT(IF(a.LogType = 'Btn1Click', 1, NULL)) AS Btn1Count
        |      ,COUNT(IF(a.LogType = 'Btn2Click', 1, NULL)) AS Btn2Count
        |      ,COUNT(IF(a.LogType = 'Btn3Click', 1, NULL)) AS Btn3Count
        |   FROM mmbi.ad_log AS a
        |   JOIN mmbi.ad_cnf AS b ON a.Token = b.Token
        |   JOIN mmbi.source_info AS c ON b.Source = c.Source AND b.GameId = c.GameId
        |   WHERE unix_timestamp(a.CreateDate,"yyyy-MM-dd'T'HH:mm:ss") >= unix_timestamp('curDay','yyyy-MM-dd') AND unix_timestamp(a.CreateDate,"yyyy-MM-dd'T'HH:mm:ss") < unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd')
        |   GROUP BY b.Source, b.GameId
        |) AS b ON a.Source=b.Source AND a.GameId=b.GameId
      """.stripMargin
        .replace("curDay", curDay)
    println(sql_hive)

    val df = hiveContext.sql(sql_hive)
    df.show()

    val sql_mysql =
      """
        |insert into statis_adlog_day(DayTime,GameId,Source,IpCount,PvCount,UvCount,Btn1Count,Btn2Count,Btn3Count) values(?,?,?,?,?,?,?,?,?)
        |on duplicate key update
        |IpCount=values(IpCount),
        |PvCount=values(PvCount),
        |UvCount=values(UvCount),
        |Btn1Count=values(Btn1Count),
        |Btn2Count=values(Btn2Count),
        |Btn3Count=values(Btn3Count)
      """.stripMargin
    println(sql_mysql)

    SparkSQLUtil.foreachPartition(df, sql_mysql)

  }

}
