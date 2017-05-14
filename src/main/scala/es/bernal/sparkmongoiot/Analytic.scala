package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
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

  def main(args: Array[String]): Unit = {

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

    //  // proyectamos la vista inicial para aplanar totalmente. Forma complicada
    //  val dateProjection: Column = udf((d: GenericRowWithSchema) => {
    //    d.getAs[Long]("epoch")
    //  }).apply(col("date")).as("date_epoch")

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

      if (isValueContinuous(rddForDs)) {
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

    ss.stop()

  }

  def isValueContinuous(data: DataFrame): Boolean = {
    val firstRow: Row = data.rdd.take(1)(0)
    try {
      val value = firstRow.getAs[String]("value")
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

}
