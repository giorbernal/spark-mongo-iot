package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

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
    .config("spark.mongodb.input.uri", "mongodb://192.168.1.209/opengate_dev2.c_og_datapoints")
    .config("spark.mongodb.output.uri", "mongodb://192.168.1.209/opengate_dev2.c_og_datapoints")
    .getOrCreate()

  println("=> config loaded!")

//  val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ss)))
//  val rdd = MongoSpark.load(ss, readConfig)
  val rdd = MongoSpark.load(ss)

  println("=> Count: " + rdd.count())
  println("=> first json: " + rdd.first())

  ss.stop()

}
