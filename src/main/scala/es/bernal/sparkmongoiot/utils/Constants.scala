package es.bernal.sparkmongoiot.utils

/**
  * Created by bernal on 27/4/17.
  */
object Constants {

  val ip: String = "192.168.1.209"
  val database: String = "poc"
  val collectionIn: String = "c_ed_datapoints"
  val collectionOut: String = "c_ed_datapoints"
  val collectionOutAgg: String = "c_ed_analytics_datapoints_section"
  val user: String = ""
  val password: String = ""

  val defaultEvalTime: Long = 3600000l // una hora
  val maxTime: Long = 1296869399628l
  val minTime: Long = 1296025107433l

  val ORG: String = "Test_organization"
  val CSV: String = ".csv"
  val PRESENCE: String = "presence"
  val STATUS: String = "status"
  val COVERAGE: String = "coverage"
  val LOCATION: String = "location"
  val GPRS_SESSION: String = "gprs_session"
  val UNDEFINED: String = "undefined"
  val INVENTORY: String = "inventory"
  val INVENTORY_IMEI: String = "imei"
  val INVENTORY_IMSI: String = "imsi"
  val INVENTORY_MANUFACTURER: String = "manufacturer"
  val INVENTORY_ICC: String = "icc"
  val INVENTORY_FIRMWARE: String = "firmware"
}
