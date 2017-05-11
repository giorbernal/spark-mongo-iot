package es.bernal.sparkmongoiot.utils

/**
  * Created by bernal on 27/4/17.
  */
object Constants {

  val ip: String = "172.19.17.111"
  val database: String = "test"
  val collectionIn: String = "test"
  val collectionOut: String = "c_ed_datapoints"
  val collectionOutAgg: String = "c_og_analytics_datapoints"

  val ORG: String = "Test_organization"
  val PRESENCE: String = "presence"
  val STATUS: String = "status"
  val LOCATION: String = "location"
  val GPRS_SESSION: String = "gprs_session"
  val UNDEFINED: String = "undefined"
  val INVENTORY: String = "inventory"
  val INVENTORY_IMEI: String = "inventory_imei"
  val INVENTORY_IMSI: String = "inventory_imsi"
  val INVENTORY_MANUFACTURER: String = "inventory_manufacturer"
  val INVENTORY_ICC: String = "inventory_icc"
  val INVENTORY_FIRMWARE: String = "invertory_firmware"
}
