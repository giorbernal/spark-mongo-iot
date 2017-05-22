package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.types._
import es.bernal.sparkmongoiot.utils.Constants
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.bson.Document

import scala.collection.mutable.ListBuffer

/**
  * Created by bernal on 25/4/17.
  */
object SimpleMongoProcess {

  def parseParams(params: Array[String]): (Double, String, String, String, String, String) = {
    if (params.length == 0) {
      //(234.5256, Constants.ip, Constants.database, Constants.collectionOutAgg, Constants.user, Constants.password)
      (234.5256, Constants.ip, Constants.database, "c_ed_datapoints_clone", Constants.user, Constants.password)
    } else if (params.length == 6) {
      (params(0).toDouble, params(1), params(2), params(3), params(4), params(5))
    } else if (params.length == 4) {
      (params(0).toDouble, params(1), params(2), params(3), "", "")
    } else {
      throw new RuntimeException("Params are not valid: " + params)
    }
  }

  def main(args: Array[String]): Unit = {

    println("=> starting Sample Mongo Process")

    val (nh: Double, ip: String, database: String, output_coll: String, user: String, pwd: String) = parseParams(args)

    val ssBuilder = SparkSession.builder()
      .appName("SampleMongoProcess")
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

    val rddMg: MongoRDD[Document] = MongoSpark.load(ss.sparkContext)
      .withPipeline(Seq(Document.parse("{ $match: { \"date.epoch\" : { $gt : " + (Constants.maxTime - nh*Constants.defaultEvalTime) + " } } }")))

    if (output_coll.equals("null")) {
      println("Number of output elements elements: " + rddMg.count())
    } else {
      MongoSpark.save(rddMg)
    }

    val timeend: Long = System.currentTimeMillis()

    println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

    ss.stop()

  }

}
