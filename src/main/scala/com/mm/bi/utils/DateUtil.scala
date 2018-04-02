package com.mm.bi.utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

/**
  * date util
  *
  * Created by kequan on 3/25/18.
  */
object DateUtil {

  val HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH")
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")
  val mm_FORMAT = new SimpleDateFormat("mm")
  val HH_FORMAT = new SimpleDateFormat("HH")


  /**
    * Convert a timestamp to a string
    *
    * @param timestamp timestamp
    * @return Date
    */
  def formatTime(timestamp: Long): String = {
    try {
      return TIME_FORMAT.format(new Date(timestamp))
    } catch {
      case e: ParseException => {
        e.printStackTrace()
        return null
      }
    }
  }

  /**
    * Convert a timestamp to a string
    *
    * @param time string
    * @return Date
    */
  def parseTime(time: String): Date = {
    try {
      return TIME_FORMAT.parse(time)
    } catch {
      case e: ParseException => {
        e.printStackTrace()
        return null
      }
    }
  }

  /**
    * Get today's date
    *
    * @return
    */
  def getTodayDate(): String = {
    return DATE_FORMAT.format(new Date());
  }

  /**
    * Get yesterday's date
    *
    * @return
    */
  def getYesterDayDate(): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    return DATE_FORMAT.format(cal.getTime);
  }

  /**
    * Get one hour before date
    *
    * @return
    */
  def getOneHourBeforeDate(): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.HOUR, -1)
    return DATE_FORMAT.format(cal.getTime);
  }


  /**
    * Get tomorrow's date
    *
    * @return
    */
  def getTomorrowDate(): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, 1)
    return DATE_FORMAT.format(cal.getTime);
  }

  /**
    *
    * Add a few days to a time : day cannot be negative
    *
    * @param time time
    * @param day  days
    * @return date
    */
  def addDay(time: String, day: Int): String = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time)
      return DATE_FORMAT.format(new Date((dateTime1.getTime + day * (24 * 60 * 60 * 1000).toLong)))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return null
      }
    }
  }

  /**
    * Subtract a time by a few days : day cannot be negative
    *
    * @param time time
    * @param day  days
    * @return date
    */
  def LessDay(time: String, day: Int): String = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time)
      return DATE_FORMAT.format(new Date((dateTime1.getTime - day * (24 * 60 * 60 * 1000).toLong)))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return null
      }
    }
  }

}
