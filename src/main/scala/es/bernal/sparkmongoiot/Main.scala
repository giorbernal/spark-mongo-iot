package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.utils.Constants
import es.bernal.sparkmongoiot.types.DataPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Column

import net.liftweb.json._

/**
  * Created by bernal on 25/4/17.
  */
object Main extends App {

//  val conf: SparkConf = new SparkConf().setAppName("spark-mongo-iot").setMaster("local")
//  val sc: SparkContext = new SparkContext(conf)

  println("=> starting spark-mongo-iot sample")

  val ss = SparkSession.builder()
    .master("local")
    .appName("spark-mongo-iot")
    .config("spark.mongodb.input.uri", "mongodb://" + Constants.ip + "/"+ Constants.database + "." + Constants.collection)
    .config("spark.mongodb.output.uri", "mongodb://" + Constants.ip + "/"+ Constants.database + "." + Constants.collection)
    .getOrCreate()
  import ss.sqlContext.implicits._

  println("=> config loaded!")

//  val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ss)))
//  val rdd = MongoSpark.load(ss, readConfig)
  val rdd = MongoSpark.load(ss).cache()
  rdd.foreach(d => {
    println(d)
  })

//  // to work with jsons, something like this ....
//  implicit val formats = DefaultFormats // Brings in default date formats etc.
//  val rddJson = rdd.toJSON.map(d => {
//    val json = parse(d)
//    json.extract[DataPoint]
//  })
//  rddJson.foreach(d => println(d.deviceId))

  ss.stop()

}
