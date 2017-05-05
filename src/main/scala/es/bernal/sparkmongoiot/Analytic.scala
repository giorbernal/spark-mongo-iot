package es.bernal.sparkmongoiot

import com.google.gson.Gson
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.utils.Constants
import es.bernal.sparkmongoiot.types.DataPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

/**
  * Created by bernal on 25/4/17.
  */
object Analytic extends App {

//  val conf: SparkConf = new SparkConf().setAppName("spark-mongo-iot").setMaster("local")
//  val sc: SparkContext = new SparkContext(conf)

  println("=> starting spark-mongo-iot sample")

  val ss = SparkSession.builder()
    .master("local")
    .appName("spark-mongo-iot")
    .config("spark.mongodb.input.uri", "mongodb://" + Constants.ip + "/"+ Constants.database + "." + Constants.collectionIn)
    .config("spark.mongodb.output.uri", "mongodb://" + Constants.ip + "/"+ Constants.database + "." + Constants.collectionOutAgg)
    .getOrCreate()
  import ss.sqlContext.implicits._

  println("=> config loaded!")

//  val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ss)))
//  val rdd = MongoSpark.load(ss, readConfig)
  val rdd = MongoSpark.load(ss)
  rdd.foreach(d => {
    println(d)
  })

//  // to work with jsons, something like this ....
//  val gson = new Gson
//  val rddJson = rdd.toJSON
//    .map(d => {
//    val json = gson.fromJson(d,classOf[DataPoint])
//    json
//  })
//  rddJson.foreach(d => println(d))

  // proyectamos la vista inicial para aplanar totalmente
  val dateProjection: Column = udf((d: GenericRowWithSchema) => {
    d.getAs[Long]("epoch")
  }).apply(col("date")).as("date_epoch")

  val rddProjected = rdd.select(col("organizationId"),col("channelId"),
    col("datastreamId"), col("deviceId"), col("feedId"), dateProjection,
    col("value")).cache

  rddProjected.show()

  // Extreemos la lista de datastreams
  rddProjected.createOrReplaceTempView("dataPointsViewPlain")
  val datastreams = ss.sql("select distinct datastreamId from dataPointsViewPlain")

  val it = datastreams.toLocalIterator()
  val dsList = new ListBuffer[String]()
  while (it.hasNext) {
    dsList += it.next().getAs[String]("datastreamId")
  }

  println(dsList.toList)

  // TODO Calculamos la agregación para cada uno de los datastrams que hemos encontrado

  ss.stop()

}
