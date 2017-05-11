package es.bernal.sparkmongoiot

import com.google.gson.Gson
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.utils.Constants
import es.bernal.sparkmongoiot.types._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.bson.Document

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

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
//  rdd.foreach(d => {
//    println(d)
//  })

//  // to work with jsons, something like this ....
//  val gson = new Gson
//  val rddJson = rdd.toJSON
//    .map(d => {
//    val json = gson.fromJson(d,classOf[DataPoint])
//    json
//  })
//  rddJson.foreach(d => println(d))

//  // proyectamos la vista inicial para aplanar totalmente. Forma complicada
//  val dateProjection: Column = udf((d: GenericRowWithSchema) => {
//    d.getAs[Long]("epoch")
//  }).apply(col("date")).as("date_epoch")

  val rddProjected = rdd.select(col("organizationId"),col("channelId"),
    col("datastreamId"), col("deviceId"), col("date.epoch").as("date_epoch"),
    col("value")).cache

  // Uso de maps en DataFrames es algo como esto ... (no me funciona)
//  val rddAux = rddProjected.map {
//    case Row(organizationId: String, channelId: String, datastreamId: String, deviceId: String, feedId: String, date_epoch: Long, value: String) =>
//      (organizationId, channelId, datastreamId, deviceId, feedId, date_epoch, value)
//  }
//  val rddAux = rddProjected

  rddProjected.show()

  // Extraemos la lista de datastreams
  val datastreams = rddProjected.select("datastreamId").distinct()

  val it = datastreams.toLocalIterator()
  val dsList = new ListBuffer[String]()
  while (it.hasNext) {
    val ds = it.next().getAs[String]("datastreamId")
    dsList += ds
  }

  val allDs = dsList.toList

  println(allDs)

  allDs.foreach(ds => {
    val rddForDs = rddProjected.where($"datastreamId" === ds).cache()

    if (isValueContinuous(rddForDs)) {
      val rddForDsAgg = rddForDs
        .groupBy("deviceId", "organizationId", "channelId", "datastreamId")
        .agg(count("value").as("count"),avg("value").as("avg"),stddev("value").as("stddev"))

      // write countinous analytic
      val rddDocs: RDD[Document] = rddForDsAgg.rdd.map(r => {
        val dpa = new DataPointAnalyticCnt(r.getAs[String]("deviceId"), r.getAs[String]("organizationId"), r.getAs[String]("channelId"),
                                        r.getAs[String]("datastreamId"),
                                        Stats(r.getAs[Long]("count"), r.getAs[Double]("avg"), r.getAs[Double]("stddev")))
        implicit val formats = DefaultFormats
        val jsonStr = write(dpa)
        Document.parse(jsonStr)
      })
      MongoSpark.save(rddDocs)
    } else {
      val rddForDsAgg = rddForDs
        .groupBy("deviceId", "organizationId", "channelId", "datastreamId", "value")
        .count()

      // write discrete analytic
      val rddForDsAggMap = rddForDsAgg.rdd.map(r => ((r.getAs[String]("deviceId"), r.getAs[String]("organizationId"), r.getAs[String]("channelId"), r.getAs[String]("datastreamId")),r))
        .groupByKey()

      val rddDocs: RDD[Document] = rddForDsAggMap.map(t => {
        var accs = ListBuffer[Accumulator]()
        t._2.foreach(r => {
          accs += Accumulator(r.getAs[String]("value"), r.getAs[Long]("count"))
        })
        val dpa = DataPointAnalyticDct(t._1._1, t._1._2, t._1._3, t._1._4, accs.toList)
//        val gson = new Gson
//        val jsonStr = gson.toJson(dpa)
        implicit val formats = DefaultFormats
        val jsonStr = write(dpa)
        Document.parse(jsonStr)
      })
      MongoSpark.save(rddDocs)
    }


  })

  ss.stop()

  def isValueContinuous(data: DataFrame): Boolean = {
    val firstRow: Row = data.rdd.take(1)(0)
    val value = firstRow.getAs[String]("value")
    try {
      value.toDouble
      true
    } catch {
      case e: NumberFormatException => {
        false
      }
      case e: NullPointerException => {
        false
      }

    }
  }

  /** *************************************************************************
    * Este es un tipo de agregacion que busca aplanar toda la estructura
    * en primera instancia. Habría que explorar como explotar esta informacion
    * @param rddProjected
    * @param allDs
    */
  def aggPlain(rddProjected: DataFrame, allDs: List[String]): Unit = {
    val ds1 = "device.dmm.location"
    val rddJoin1 = getDsJoinPart(rddProjected, ds1, allDs)
    rddJoin1.show

    val ds2 = "device.dmm.path"
    val rddJoin2 = getDsJoinPart(rddProjected, ds2, allDs)
    rddJoin2.show

    val ds3 = "device.dmm.operationalStatus"
    val rddJoin3 = getDsJoinPart(rddProjected, ds3, allDs)
    rddJoin3.show

    val rddJoined = rddJoin1.union(rddJoin2).union(rddJoin3)

    rddJoined.show()

    rddJoined.createOrReplaceTempView("datastream_joined")
    //val agg = ss.sql("select count(device.dmm.location), count(device.dmm.operationalStatus) from datastream_joined")

    //agg.show()
  }

  def getDsJoinPart(rddProjected: DataFrame, dsName: String, allDs: List[String]): DataFrame = {
    val rddFiltered = rddProjected.filter($"datastreamId" === dsName)
    val rddAdapted = rddFiltered.select(col("organizationId"),col("channelId"),
      col("deviceId"), col("feedId"), col("date_epoch"), col("value"))

    var dsDf = rddAdapted

    for (i <- 0 until allDs.length) {
      val dsAux = allDs(i)
      if (dsAux.equals(dsName)) {
        //        val dsProjection: Column = udf((v: String) => {
        //          v
        //        }).apply(col("value"))
        dsDf = dsDf.withColumn(dsName, col("value"))
      } else {
        dsDf = dsDf.withColumn(dsAux,lit(null:String))
      }
    }
    dsDf
  }

}
