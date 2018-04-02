package com.mm.bi.publish

import com.mm.bi.singleton.HiveContextSingleton
import com.mm.bi.utils.{DateUtil, SparkConfUtil, SparkSQLUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * statis_round_tax_count_day
  * Created by kequan on 3/25/18.
  */
object StatisRoundTaxCountDay {

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
         SELECT
        |   'curDay' AS DayTime,
        |   gi.GameId AS GameId,
        |   si.Source AS Source,
        |   NVL (CjTaxUc, 0) AS CjTaxUc,
        |   NVL (CjTaxC, 0) AS CjTaxC,
        |   NVL (ZjTaxUc, 0) AS ZjTaxUc,
        |   NVL (ZjTaxC, 0) AS ZjTaxC,
        |   NVL (GjTaxUc, 0) AS GjTaxUc,
        |   NVL (GjTaxC, 0) AS GjTaxC,
        |   NVL (DjTaxUc, 0) AS DjTaxUc,
        |   NVL (DjTaxC, 0) AS DjTaxC,
        |   NVL (BrTaxUc, 0) AS BrTaxUc,
        |   NVL (BrTaxC, 0) AS BrTaxC,
        |   NVL (GsTaxUc, 0) AS GsTaxUc,
        |   NVL (GsTaxC, 0) AS GsTaxC,
        |   NVL (LfTaxUc, 0) AS LfTaxUc,
        |   NVL (LfTaxC, 0) AS LfTaxC,
        |   NVL (TrTaxUc, 0) AS TrTaxUc,
        |   NVL (TrTaxC, 0) AS TrTaxC,
        |   NVL (FqTaxUc, 0) AS FqTaxUc,
        |   NVL (FqTaxC, 0) AS FqTaxC,
        |   NVL (ZjhTaxUc, 0) AS ZjhTaxUc,
        |   NVL (ZjhTaxC, 0) AS ZjhTaxC,
        |   NVL (ByTaxUc, 0) AS ByTaxUc,
        |   NVL (ByTaxC, 0) AS ByTaxC,
        |   NVL (WrnnTaxUc, 0) AS WrnnTaxUc,
        |   NVL (WrnnTaxC, 0) AS WrnnTaxC,
        |   's1' AS PlayWear
        |FROM  (select distinct GameId,appid from mmbi.game_info where GameId='game_id') gi
        |JOIN (select * from mmbi.source_info WHERE level=1) si ON gi.GameId = si.GameId
        |LEFT JOIN (
        |        SELECT
        |        b.appid,
        |        b.channel_id AS Source,
        |        count(DISTINCT if(a.system_type = 16101,a.uid,null)) CjTaxUc,
        |        count(if(a.system_type = 16101,1,null)) CjTaxC,
        |        count(DISTINCT if(a.system_type = 16102,a.uid,null)) ZjTaxUc,
        |        count(if(a.system_type = 16102,1,null)) ZjTaxC,
        |        count(DISTINCT if(a.system_type = 16103,a.uid,null)) GjTaxUc,
        |        count(if(a.system_type = 16103,1,null)) GjTaxC,
        |        count(DISTINCT if(a.system_type = 16104,a.uid,null)) DjTaxUc,
        |        count(if(a.system_type = 16104,1,null)) DjTaxC,
        |        count(DISTINCT if(a.system_type = 22,a.uid,null)) BrTaxUc,
        |        count(if(a.system_type = 22,1,null)) BrTaxC,
        |        count(DISTINCT if(a.system_type = 41,a.uid,null)) GsTaxUc,
        |        count(if(a.system_type = 41,1,null)) GsTaxC,
        |        count(DISTINCT if(a.system_type = 30,a.uid,null)) LfTaxUc,
        |        count(if(a.system_type = 30,1,null)) LfTaxC,
        |        count(DISTINCT if(a.system_type = 71,a.uid,null)) TrTaxUc,
        |        count(if(a.system_type = 71,1,null)) TrTaxC,
        |        count(DISTINCT if(a.system_type = 21,a.uid,null)) FqTaxUc,
        |        count(if(a.system_type = 21,1,null)) FqTaxC,
        |        count(DISTINCT if(a.system_type = 40,a.uid,null)) ZjhTaxUc,
        |        count(if(a.system_type = 40,1,null)) ZjhTaxC,
        |        count(DISTINCT if(a.system_type = 80,a.uid,null)) ByTaxUc,
        |        count(if(a.system_type = 80,1,null)) ByTaxC,
        |        count(DISTINCT if(a.system_type = 72,a.uid,null)) WrnnTaxUc,
        |        count(if(a.system_type = 72,1,null)) WrnnTaxC
        |   FROM (select * from log_pump where unix_timestamp(dt,'yyyyMMdd')>=unix_timestamp('curDay','yyyy-MM-dd') and unix_timestamp(dt,'yyyyMMdd')<=unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd')) a
        |   JOIN player b ON a.uid = b.id
        |   WHERE a.trigger_time >= unix_timestamp('curDay', 'yyyy-MM-dd') * 1000
        |      AND a.trigger_time < unix_timestamp(cast(date_add('curDay', 1) as string), 'yyyy-MM-dd') * 1000
        |   GROUP BY b.appid, b.channel_id
        |) AS ab ON gi.appid = ab.appid AND si.Source = ab.Source
      """.stripMargin
        .replace("curDay", curDay).replace("game_id",game_id)
    println(sql_hive)

    val df = hiveContext.sql(sql_hive)
    df.show()

    val sql_mysql =
      """
        |insert into statis_round_tax_count_day(DayTime,GameId,Source,CjTaxUc,CjTaxC,ZjTaxUc,ZjTaxC,GjTaxUc,GjTaxC,DjTaxUc,DjTaxC,BrTaxUc,BrTaxC,GsTaxUc,GsTaxC,LfTaxUc,LfTaxC,TrTaxUc,TrTaxC,FqTaxUc,FqTaxC,ZjhTaxUc,ZjhTaxC,ByTaxUc, ByTaxC,WrnnTaxUc,WrnnTaxC,PlayWear) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        |on duplicate key  update
        |CjTaxUc=values(CjTaxUc),
        |CjTaxC=values(CjTaxC),
        |ZjTaxUc=values(ZjTaxUc),
        |ZjTaxC=values(ZjTaxC),
        |GjTaxUc=values(GjTaxUc),
        |GjTaxC=values(GjTaxC),
        |DjTaxUc=values(DjTaxUc),
        |DjTaxC=values(DjTaxC),
        |BrTaxUc=values(BrTaxUc),
        |BrTaxC=values(BrTaxC),
        |GsTaxUc=values(GsTaxUc),
        |GsTaxC=values(GsTaxC),
        |LfTaxUc=values(LfTaxUc),
        |LfTaxC=values(LfTaxC),
        |TrTaxUc=values(TrTaxUc),
        |TrTaxC=values(TrTaxC),
        |FqTaxUc=values(FqTaxUc),
        |FqTaxC=values(FqTaxC),
        |ZjhTaxUc=values(ZjhTaxUc),
        |ZjhTaxC=values(ZjhTaxC),
        |ByTaxUc=values(ByTaxUc),
        |ByTaxC=values(ByTaxC),
        |WrnnTaxUc=values(WrnnTaxUc),
        |WrnnTaxC=values(WrnnTaxC)
      """.stripMargin
    println(sql_mysql)

    SparkSQLUtil.foreachPartition(df, sql_mysql)

  }

}
