package es.bernal.sparkmongoiot.types

/**
  * Created by bernal on 27/4/17.
  */
trait DataPoint {
  val feedId: String
  val datastreamId: String
  val deviceId: String
  val organizationId: String
  val channelId: String
  val date: DsTime
  val from: DsTime
}

case class DataPointDct (val feedId: String, val datastreamId: String, val deviceId: String,
                     val organizationId: String, val channelId: String,
                     val date: DsTime, val from: DsTime, val value: String) extends DataPoint {
}

case class DataPointCnt (val feedId: String, val datastreamId: String, val deviceId: String,
                         val organizationId: String, val channelId: String,
                         val date: DsTime, val from: DsTime, val value: Double) extends DataPoint {
}

case class DsTime(val epoch: Long, val year: Double, val month: Double,
             val day: Double, val weekday: Double, val yearday: Double,
             val yearweek: Double, val monthweek: Double,
             val dstoffset: Double, val hour: Double, val minute: Double, val second: Double)
