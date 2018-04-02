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
object StatisPaidAnalysisDay {

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
      forEachGame(hiveContext, key, value,"2018-03-25")
    }

  }

  def forEachGame(hiveContext: HiveContext, gameId: String, db: String,curDay:String) = {

    hiveContext.sql("use " + db)
    val sql_hive =
      """
        |SELECT 'curDay' AS DayTime
        |					,gi.gameId as GameId
        |					,b.channel_id AS Source
        |					,COUNT(DISTINCT a.player_id) AS PayCount
        |					,SUM(a.amount) AS PayMoney
        |					,COUNT(DISTINCT IF(b.reg_time >= unix_timestamp('curDay','yyyy-MM-dd') AND b.reg_time < unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd'),a.player_id,NULL)) AS NewPayCount
        |					,SUM(IF(b.reg_time >= unix_timestamp('curDay','yyyy-MM-dd') AND b.reg_time < unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd'),a.amount,0)) AS NewPayMoney
        |					,COUNT(DISTINCT IF(b.reg_time < unix_timestamp('curDay','yyyy-MM-dd') OR b.reg_time >= unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd'),a.player_id,NULL)) AS OldPayCount
        |					,SUM(IF(b.reg_time < unix_timestamp('curDay','yyyy-MM-dd') OR b.reg_time >= unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd'),a.amount,0)) AS PldPayMoney
        |					,pi.PlayWear AS PlayWear
        |FROM mmbi.game_info gi
        |JOIN mmbi.playwear_info pi ON gi.GameId = pi.GameId
        |JOIN (
        |     select * from log_player_pay
        |     WHERE unix_timestamp(dt,'yyyyMMdd') >= unix_timestamp('curDay','yyyy-MM-dd')
        |     AND unix_timestamp(dt,'yyyyMMdd') <= unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd')
        |)AS a ON a.AppId = gi.AppId
        |JOIN player AS b ON a.player_id = b.id and b.AppId = gi.AppId
        |WHERE a.pay_time >= unix_timestamp('curDay','yyyy-MM-dd')
        |	AND a.pay_time < unix_timestamp(cast(date_add('curDay',1) as string),'yyyy-MM-dd')
        |	AND a.pay_status=1
        |GROUP BY  gi.GameId,b.channel_id,pi.playwear
      """.stripMargin
        .replace("curDay", curDay)

    val df = hiveContext.sql(sql_hive)

    val sql_mysql =
      """
        |insert into statis_paid_analysis_day(DayTime,GameId,Source,PayCount,PayMoney,NewPayCount,NewPayMoney,OldPayCount,OldPayMoney,PlayWear) values(?,?,?,?,?,?,?,?,?,?)
        |on duplicate key update
        |DayTime=values(DayTime),
        |GameId=values(GameId),
        |Source=values(Source),
        |PayCount=values(PayCount),
        |PayMoney=values(PayMoney),
        |NewPayCount=values(NewPayCount),
        |NewPayMoney=values(NewPayMoney),
        |OldPayCount=values(OldPayCount),
        |OldPayMoney=values(OldPayMoney),
        |PlayWear=values(PlayWear)
      """.stripMargin
    SparkSQLUtil.foreachPartitionBatch(df, sql_mysql)
    df.show(1000)
    println(sql_hive)
  }
}
