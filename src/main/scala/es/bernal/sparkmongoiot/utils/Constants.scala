package es.bernal.sparkmongoiot.utils

/**
  * Created by bernal on 27/4/17.
  */
object Constants {

  val ip: String = "localhost"
  val database: String = "poc"
  val collectionIn: String = "c_ed_datapoints"
  val collectionOut: String = "c_ed_datapoints"
  val collectionOutAgg: String = "c_ed_analytics_datapoints"

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
