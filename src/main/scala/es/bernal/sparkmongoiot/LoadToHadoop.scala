package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.utils.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bson.{BSONObject, Document}

/**
  * Created by bernal on 25/4/17.
  */
object LoadToHadoop {

  def parseParams(params: Array[String]): (Double, String, String, String, String, String, String, String) = {
    if (params.length == 0) {
      (240, Constants.ip, Constants.database, Constants.hdfsFile, Constants.user, Constants.password, Constants.hdfsHostPort, Constants.modeMongoConn)
    } else if (params.length == 8) {
      (params(0).toDouble, params(1), params(2), params(3), params(4), params(5), params(6), params(7))
    } else {
      throw new RuntimeException("Params are not valid: " + params)
    }
  }

  def main(args: Array[String]): Unit = {

    val (nh: Double, ip: String, database: String, hdfsPath: String, user: String, pwd: String, hdfsHostPort: String, mode: String) = parseParams(args)

    println("=> starting LoadToHadoop application. mode: " + mode)

    if (mode.equals(Constants.modeHadoopConn)) {
      // Hadoop MongoDB Connector management

      val conf = new SparkConf().setAppName("LoadToHadoop")
      val config = new Configuration()
      config.set("mongo.input.uri", "mongodb://" + user + ":" + pwd + "@" + ip + "/" + database + "." + Constants.collectionIn)

      if (args.length == 0) {
        conf.setMaster("local")
      }
      val sc = new SparkContext(conf)

      config.set("mongo.job.input.format","com.mongodb.hadoop.MongoInputFormat")

      val keyClassName = classOf[Object]
      val valueClassName = classOf[BSONObject]
      val inputFormatClassName = classOf[com.mongodb.hadoop.MongoInputFormat]
      val ipRDD = sc.newAPIHadoopRDD(config,inputFormatClassName,keyClassName,valueClassName)

      val timeini: Long = System.currentTimeMillis()

      if (args.length == 0)
        ipRDD.saveAsTextFile(Constants.my_hdfs_fs + "/" + hdfsPath)
      else
        ipRDD.saveAsTextFile("hdfs://" + hdfsHostPort + "/" + hdfsPath)

      val timeend: Long = System.currentTimeMillis()

      println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

      sc.stop()

    } else if (mode.equals(Constants.modeMongoConn)) {
      // Spark MongoDB Connector management

      val ssBuilder = SparkSession.builder()
        .appName("LoadToHadoop")
        .config("spark.mongodb.input.uri", "mongodb://" + user + ":" + pwd + "@" + ip + "/" + database + "." + Constants.collectionIn)
      if (args.length == 0) {
        ssBuilder
          .master("local")
      }
      val ss = ssBuilder.getOrCreate()

      println("=> config loaded!")
      val timeini: Long = System.currentTimeMillis()

      val rddMg: MongoRDD[Document] = MongoSpark.load(ss.sparkContext)
        .withPipeline(Seq(Document.parse("{ $match: { \"date.epoch\" : { $gt : " + (Constants.maxTime - nh*Constants.defaultEvalTime) + " } } }")))

      if (args.length == 0)
        rddMg.map(d => d.toJson).saveAsTextFile(Constants.my_hdfs_fs +"/" + hdfsPath)
      else
        rddMg.map(d => d.toJson).saveAsTextFile("hdfs://" + hdfsHostPort + "/" + hdfsPath)

      val timeend: Long = System.currentTimeMillis()

      println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

      ss.stop()

    } else {
      println("mode param error: " + mode)
    }
  }

}
