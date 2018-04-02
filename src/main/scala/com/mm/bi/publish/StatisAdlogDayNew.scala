package com.mm.bi.publish

import com.mm.bi.singleton.HiveContextSingleton
import com.mm.bi.utils.{DateUtil, SparkConfUtil, SparkSQLUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * statis_adlog_day_new
  * Created by kequan on 3/28/18.
  */
object StatisAdlogDayNew {

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
        |  a.GameId,
        |  a.Source,
        |  NVL(IpCount,0) AS IpCount,
        |  NVL(PvCount,0) AS PvCount,
        |  NVL(UvCount,0) AS UvCount,
        |  NVL(WindowsCount,0) AS WindowsCount,
        |  NVL(IOSCount,0) AS IOSCount,
        |  NVL(AndroidCount,0) As AndroidCount
        |FROM mmbi.source_info AS a
        |LEFT JOIN (
        |  SELECT b.Source,b.GameId
        |    ,COUNT(DISTINCT a.RefererIp) AS IpCount
        |    ,COUNT(*) AS PvCount
        |    ,COUNT(DISTINCT a.pvi) AS UvCount
        |    ,COUNT(IF(a.os = 'Windows', 1, NULL)) AS WindowsCount
        |    ,COUNT(IF(a.os = 'iPhone/iPod' or os = 'iPad', 1, NULL)) AS IOSCount
        |    ,COUNT(IF(a.os = 'Android', 1, NULL)) AS AndroidCount
        |  FROM mmbi.ad_log_new AS a
        |  JOIN mmbi.ad_cnf AS b ON a.Token = b.Token
        |  JOIN mmbi.source_info AS c ON b.Source = c.Source AND b.GameId = c.GameId
        |  WHERE unix_timestamp(a.CreateDate,"yyyy-MM-dd'T'HH:mm:ss") >= unix_timestamp('curDay','yyyy-MM-dd') AND unix_timestamp(a.CreateDate,"yyyy-MM-dd'T'HH:mm:ss") < unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd')
        |  GROUP BY b.Source, b.GameId
        |) AS b ON a.Source=b.Source AND a.GameId=b.GameId
      """.stripMargin
        .replace("curDay", curDay)
    println(sql_hive)

    val df = hiveContext.sql(sql_hive)
    df.show()

    val sql_mysql =
      """
        insert into statis_adlog_day_new(DayTime,GameId,Source,IpCount,PvCount,UvCount,WindowsCount,IOSCount,AndroidCount) values(?,?,?,?,?,?,?,?,?)
        |on duplicate key update
        |IpCount=values(IpCount),
        |PvCount=values(PvCount),
        |UvCount=values(UvCount),
        |WindowsCount=values(WindowsCount),
        |IOSCount=values(IOSCount),
        |AndroidCount=values(AndroidCount)
      """.stripMargin
    println(sql_mysql)

    SparkSQLUtil.foreachPartition(df, sql_mysql)

  }

}
