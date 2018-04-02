package com.mm.bi.publish

import com.mm.bi.singleton.HiveContextSingleton
import com.mm.bi.utils.{DateUtil, SparkConfUtil, SparkSQLUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * statis_round_day.job
  * Created by kequan on 3/25/18.
  */
object StatisRoundDay {

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
        |   SELECT
        |   'curDay' AS DayTime,
        |   gi.GameId AS GameId,
        |   si.Source AS Source,
        |   NVL (CjCount, 0) AS CjCount,
        |   NVL (ZjCount, 0) AS ZjCount,
        |   NVL (GjCount, 0) AS GjCount,
        |   NVL (DjCount, 0) AS DjCount,
        |   0 AS IVCount,
        |   NVL (XCount, 0) AS XCount,
        |   NVL (GsCount, 0) AS GsCount,
        |   NVL (LfCount, 0) AS LfCount,
        |   NVL (TrCount, 0) AS TrCount,
        |   NVL (FqCount, 0) AS FqCount,
        |   's1' AS PlayWear
        |FROM (select distinct GameId,appid from mmbi.game_info where GameId='game_id') gi
        |JOIN (select * from mmbi.source_info WHERE level=1) si ON gi.GameId = si.GameId
        |LEFT JOIN (
        |        SELECT
        |        b.appid,
        |        b.channel_id AS Source,
        |        count(DISTINCT if(a.system_type = 16101,a.uid,null)) CjCount,
        |        count(DISTINCT if(a.system_type = 16102,a.uid,null)) ZjCount,
        |        count(DISTINCT if(a.system_type = 16103,a.uid,null)) GjCount,
        |        count(DISTINCT if(a.system_type = 16104,a.uid,null)) DjCount,
        |        count(DISTINCT if(a.system_type = 22,a.uid,null)) XCount,
        |        count(DISTINCT if(a.system_type = 41,a.uid,null)) GsCount,
        |        count(DISTINCT if(a.system_type = 30,a.uid,null)) LfCount,
        |        count(DISTINCT if(a.system_type = 71,a.uid,null)) TrCount,
        |        count(DISTINCT if(a.system_type = 21,a.uid,null)) FqCount
        |   FROM (select * from  log_coin where unix_timestamp(dt,'yyyyMMdd')>=unix_timestamp('curDay','yyyy-MM-dd') and unix_timestamp(dt,'yyyyMMdd')<=unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd')) a
        |   JOIN player b ON a.uid = b.id
        |   WHERE a.trigger_time >= unix_timestamp('curDay', 'yyyy-MM-dd') * 1000
        |      AND a.trigger_time<unix_timestamp(cast(date_add('curDay', 1) as string), 'yyyy-MM-dd') * 1000
        |   GROUP BY b.appid, b.channel_id
        |) AS ab ON gi.appid = ab.appid AND si.Source = ab.Source
      """.stripMargin
        .replace("curDay", curDay).replace("game_id",game_id)
    println(sql_hive)

    val df = hiveContext.sql(sql_hive)
    df.show()

    val sql_mysql =
      """
        |insert into statis_round_day(DayTime,GameId,Source,CjCount,ZjCount,GjCount,DjCount,IVCount, XCount,GsCount,LfCount, TrCount, FqCount,PlayWear) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        |on duplicate key update
        |CjCount=values(CjCount),
        |ZjCount=values(ZjCount),
        |GjCount=values(GjCount),
        |DjCount=values(DjCount),
        |IVCount=values(IVCount),
        |XCount=values(XCount),
        |LfCount=values(LfCount),
        |TrCount=values(TrCount),
        |FqCount=values(FqCount)
      """.stripMargin
    println(sql_mysql)

    SparkSQLUtil.foreachPartition(df, sql_mysql)

  }

}
