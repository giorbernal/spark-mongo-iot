package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.utils.Constants
import es.bernal.sparkmongoiot.types._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.bson.Document

import scala.collection.mutable.ListBuffer

/**
  * Created by bernal on 25/4/17.
  */
object Analytic {

  def parseParams(params: Array[String]): (Double, String, String, String, String, String) = {
    if (params.length == 0) {
      (234.5256, Constants.ip, Constants.database, Constants.collectionOutAgg, Constants.user, Constants.password)
    } else if (params.length == 6) {
      (params(0).toDouble, params(1), params(2), params(3), params(4), params(5))
    } else if (params.length == 4) {
      (params(0).toDouble, params(1), params(2), params(3), "", "")
    } else {
      throw new RuntimeException("Params are not valid: " + params)
    }
  }

  def main(args: Array[String]): Unit = {

    println("=> starting spark-mongo-iot sample")

    val (nh: Double, ip: String, database: String, output_coll: String, user: String, pwd: String) = parseParams(args)

    val ssBuilder = SparkSession.builder()
      .appName("spark-mongo-iot")
    if (args.length == 0) {
      ssBuilder
        .master("local[*]")
    }
    if (user.equals("") && pwd.equals("")) {
      ssBuilder
        .config("spark.mongodb.input.uri", "mongodb://" + ip + "/"+ database + "." + Constants.collectionIn)
        .config("spark.mongodb.output.uri", "mongodb://" + ip + "/"+ database + "." + output_coll)
    } else {
      ssBuilder
        .config("spark.mongodb.input.uri", "mongodb://" + user + ":" + pwd + "@" + ip + "/" + database + "." + Constants.collectionIn)
        .config("spark.mongodb.output.uri", "mongodb://" + user + ":" + pwd + "@" + ip + "/" + database + "." + output_coll)
    }
    val ss = ssBuilder.getOrCreate()
    import ss.sqlContext.implicits._

    println("=> config loaded!")
    val timeini: Long = System.currentTimeMillis()

    //  val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ss)))
    //  val rdd = MongoSpark.load(ss, readConfig)
    val rddMg: MongoRDD[Document] = MongoSpark.load(ss.sparkContext)
      .withPipeline(Seq(Document.parse("{ $match: { \"date.epoch\" : { $gt : " + (Constants.maxTime - nh*Constants.defaultEvalTime) + " } } }")))
    val rdd = rddMg.toDF

    val rddProjected = rdd.select(col("organizationId"),col("channelId"),
      col("datastreamId"), col("deviceId"), col("date.epoch").as("date_epoch"),
      col("value")).cache

    // Extraemos la lista de datastreams
    val datastreams = rddProjected.select("datastreamId").distinct()

    val it = datastreams.toLocalIterator()
    val dsList = new ListBuffer[String]()
    while (it.hasNext) {
      val ds = it.next().getAs[String]("datastreamId")
      dsList += ds
    }

    val allDs = dsList.toList

    allDs.foreach(ds => {
      val rddForDs = rddProjected.where($"datastreamId" === ds)

      if (isValueContinuous(ds)) {
        val rddForDsAgg = rddForDs
          .groupBy("deviceId", "organizationId", "channelId", "datastreamId")
          .agg(count("value").as("count"),avg("value").as("avg"),stddev("value").as("stddev")
            ,max("value").as("max"), min("value").as("min")
          )

        // write countinous analytic
        val rddDocs: RDD[Document] = rddForDsAgg.rdd.map(r => {
          val dpa = new DataPointAnalyticCnt(r.getAs[String]("deviceId"), r.getAs[String]("organizationId"), r.getAs[String]("channelId"),
            r.getAs[String]("datastreamId"),
            Stats(r.getAs[Long]("count"), r.getAs[Double]("avg"), r.getAs[Double]("stddev")
              , r.getAs[String]("max").toDouble, r.getAs[String]("min").toDouble
            ))
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
          implicit val formats = DefaultFormats
          val jsonStr = write(dpa)
          Document.parse(jsonStr)
        })
        MongoSpark.save(rddDocs)
      }

    })

    val timeend: Long = System.currentTimeMillis()

    println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

    ss.stop()

  }

  def isValueContinuous(ds: String): Boolean = {
    if (ds.equals("coverage"))
      true
    else
      false
  }


}
