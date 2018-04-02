package com.mm.bi.publish

import com.mm.bi.singleton.HiveContextSingleton
import com.mm.bi.utils.{DateUtil, SparkConfUtil, SparkSQLUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * statis_round_tax_day.job
  * Created by kequan on 3/25/18.
  */
object StatisRoundTaxDay {

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

    // GameId -> alias
    val map: Map[String, String] = Map("com.leishen.kuaiyin" -> "ddz", "com.leishen.laokyule" -> "lk_ddz")
    for ((key, value) <- map) {
      forEachGame(hiveContext, key, value)
    }

  }

  def forEachGame(hiveContext: HiveContext, game_id: String, db: String) = {

    hiveContext.sql("use " + db)
    val sql_hive =
      """
        |SELECT
        |   'curDay' AS DayTime,
        |   gi.GameId AS GameId,
        |   si.Source AS Source,
        |   NVL (CjTax, 0) AS CjTax,
        |   NVL (ZjTax, 0) AS ZjTax,
        |   NVL (GjTax, 0) AS GjTax,
        |   NVL (DjTax, 0) AS DjTax,
        |   NVL (BrTax, 0) AS BrTax,
        |   NVL (GsTax, 0) AS GsTax,
        |   NVL (LfTax, 0) AS LfTax,
        |   NVL (TrTax, 0) AS TrTax,
        |   NVL (FqTax, 0) AS FqTax,
        |   NVL (ZjhTax, 0) AS ZjhTax,
        |   NVL (ByTax, 0) AS ByTax,
        |   NVL (WrnnTax, 0) AS WrnnTax,
        |   's1' AS PlayWear
        |FROM  (select distinct GameId,appid from mmbi.game_info where GameId='game_id') gi
        |JOIN (select * from mmbi.source_info WHERE level=1) si ON gi.GameId = si.GameId
        |LEFT JOIN (
        |        SELECT
        |        b.appid,
        |        b.channel_id AS Source,
        |        SUM(if(a.system_type = 16101,a.coin,0)) CjTax,
        |        SUM(if(a.system_type = 16102,a.coin,0)) ZjTax,
        |        SUM(if(a.system_type = 16103,a.coin,0)) GjTax,
        |        SUM(if(a.system_type = 16104,a.coin,0)) DjTax,
        |        SUM(if(a.system_type = 22,a.coin,0)) BrTax,
        |        SUM(if(a.system_type = 41,a.coin,0)) GsTax,
        |        SUM(if(a.system_type = 30,a.coin,0)) LfTax,
        |        SUM(if(a.system_type = 71,a.coin,0)) TrTax,
        |        SUM(if(a.system_type = 21,a.coin,0)) FqTax,
        |        SUM(if(a.system_type = 40,a.coin,0)) ZjhTax,
        |        SUM(if(a.system_type = 80,a.coin,0)) ByTax,
        |        SUM(if(a.system_type = 72,a.coin,0)) WrnnTax
        |   FROM (select * from log_pump where unix_timestamp(dt,'yyyyMMdd')>=unix_timestamp('curDay','yyyy-MM-dd') and unix_timestamp(dt,'yyyyMMdd')<=unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd')) a
        |   JOIN player b ON a.uid = b.id
        |   WHERE a.trigger_time >= unix_timestamp('curDay', 'yyyy-MM-dd') * 1000
        |      AND a.trigger_time < unix_timestamp(cast(date_add('curDay', 1) as string), 'yyyy-MM-dd') * 1000
        |   GROUP BY b.appid, b.channel_id
        |) AS ab ON gi.appid = ab.appid AND si.Source = ab.Source
      """.stripMargin
        .replace("curDay", curDay).replace("game_id", game_id)
    println(sql_hive)

    val df = hiveContext.sql(sql_hive)
    df.show()

    val sql_mysql =
      """
        |insert into statis_round_tax_day(DayTime,GameId,Source,CjTax,ZjTax,GjTax,DjTax,BrTax,GsTax,LfTax,TrTax,FqTax,ZjhTax,ByTax,WrnnTax,PlayWear) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        |on duplicate key update
        |CjTax=values(CjTax),
        |ZjTax=values(ZjTax),
        |GjTax=values(GjTax),
        |DjTax=values(DjTax),
        |BrTax=values(BrTax),
        |GsTax=values(GsTax),
        |LfTax=values(LfTax),
        |TrTax=values(TrTax),
        |FqTax=values(FqTax),
        |ZjhTax=values(ZjhTax),
        |ByTax=values(ByTax),
        |WrnnTax=values(WrnnTax)
      """.stripMargin
    println(sql_mysql)

    SparkSQLUtil.foreachPartition(df, sql_mysql)

  }

}
