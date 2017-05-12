package es.bernal.sparkmongoiot.utils

/**
  * Created by bernal on 27/4/17.
  */
object Constants {

  val ip: String = "172.19.17.111"
  val database: String = "test"
  val collectionIn: String = "test"
  val collectionOut: String = "c_ed_datapoints"
  val collectionOutAgg: String = "c_tt_datapoints"

  val ORG: String = "Test_organization"
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
