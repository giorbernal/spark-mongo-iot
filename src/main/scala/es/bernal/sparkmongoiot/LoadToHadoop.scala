package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.types._
import es.bernal.sparkmongoiot.utils.Constants
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.bson.{BSONObject, Document}

import scala.collection.mutable.ListBuffer

/**
  * Created by bernal on 25/4/17.
  */
object LoadToHadoop {

  def parseParams(params: Array[String]): (String, String, String, String, String, String) = {
    if (params.length == 0) {
      (Constants.ip, Constants.database, Constants.hdfsPath, Constants.user, Constants.password, "HADOOP")
    } else if (params.length == 6) {
      (params(0), params(1), params(2), params(3), params(4), params(5))
    } else {
      throw new RuntimeException("Params are not valid: " + params)
    }
  }

  def main(args: Array[String]): Unit = {

    val (ip: String, database: String, hdfsPath: String, user: String, pwd: String, mode: String) = parseParams(args)

    println("=> starting LoadToHadoop application. mode: " + mode)

    if (mode.equals(Constants.modeHadoopConn)) {
      val conf = new SparkConf().setAppName("LoadToHadoop")
      if (args.length == 0)
        conf.setMaster("local")
      val sc = new SparkContext(conf)

      val config = new Configuration()
      config.set("mongo.job.input.format","com.mongodb.hadoop.MongoInputFormat")
      config.set("mongo.input.uri", "mongodb://" + user + ":" + pwd + "@" + ip + "/" + database + "." + Constants.collectionIn)

      val keyClassName = classOf[Object]
      val valueClassName = classOf[BSONObject]
      val inputFormatClassName = classOf[com.mongodb.hadoop.MongoInputFormat]
      val ipRDD = sc.newAPIHadoopRDD(config,inputFormatClassName,keyClassName,valueClassName)

      val timeini: Long = System.currentTimeMillis()

      if (args.length == 0)
        ipRDD.saveAsTextFile("output/" + hdfsPath)
      else
        ipRDD.saveAsTextFile("hdfs://" + hdfsPath)

      val timeend: Long = System.currentTimeMillis()

      println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

      sc.stop()

    } else if (mode.equals(Constants.modeMongoConn)) {
      val ssBuilder = SparkSession.builder()
        .appName("LoadToHadoop")
        .config("spark.mongodb.input.uri", "mongodb://" + user + ":" + pwd + "@" + ip + "/" + database + "." + Constants.collectionIn)
      if (args.length == 0)
        ssBuilder.master("local")
      val ss = ssBuilder.getOrCreate()
      import ss.sqlContext.implicits._

      println("=> config loaded!")
      val timeini: Long = System.currentTimeMillis()

      val rddMg: MongoRDD[Document] = MongoSpark.load(ss.sparkContext)
        .withPipeline(Seq(Document.parse("{ $match: { \"date.epoch\" : { $gt : " + (Constants.maxTime - 240*Constants.defaultEvalTime) + " } } }")))

      if (args.length == 0)
        rddMg.saveAsTextFile("output/" + hdfsPath)
      else
        rddMg.saveAsTextFile("hdfs://" + hdfsPath)

      val timeend: Long = System.currentTimeMillis()

      println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

      ss.stop()

    } else {
      println("mode param error: " + mode)
    }
  }

}
