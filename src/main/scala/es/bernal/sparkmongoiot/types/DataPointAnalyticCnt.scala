package es.bernal.sparkmongoiot.types

/**
  * Created by bernal on 10/5/17.
  */

case class DataPointAnalyticCnt(val deviceId: String, val organizationId: String, val channelId: String,
                                val datastreamId: String, val stats: Stats)

case class Stats(val count: Long, val avg: Double, val stddev: Double
                , val max: Double, val min: Double
                )
