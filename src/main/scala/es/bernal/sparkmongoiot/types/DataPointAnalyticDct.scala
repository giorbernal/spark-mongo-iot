package es.bernal.sparkmongoiot.types

/**
  * Created by bernal on 10/5/17.
  */

case class DataPointAnalyticDct(val deviceId: String, val organizationId: String, val channelId: String,
                                val datastreamId: String, val accumulators: List[Accumulator])

case class Accumulator(val name: String, val count: Long)