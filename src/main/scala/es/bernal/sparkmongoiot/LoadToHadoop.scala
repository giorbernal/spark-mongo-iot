package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.utils.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.bson.{BSONObject, Document}

/**
  * Created by bernal on 25/4/17.
  */
object LoadToHadoop {

  // Basic parameter classes
  trait Environment
  case class Local(
                    val hours: Double = 234.5256,
                    val ip: String = Constants.ip,
                    val user: String = Constants.user,
                    val password: String = Constants.password,
                    val database: String = Constants.database,
                    val hdfsFile: String = Constants.hdfsFile,
                    val hdfsHostPort: String = Constants.hdfsHostPort
                  ) extends Environment
  case class RemoteCluster(val params: Array[String]) extends Environment

  // Type Class interface
  trait Configurator[T] {
    def getHours(t: T): Double
    def getMongoUrl(t: T): String
    def getHadoopConfig(t: T): (Configuration, SparkConf)
    def getSparkSessionBuilder(t: T): SparkSession.Builder
    def hadoopPersistanceFromHD(rdd: RDD[(Object, org.bson.BSONObject)], t: T): Unit
    def hadoopPersistanceFromSD(rdd: MongoRDD[Document], t: T): Unit
  }

  object Configurator extends ConfiguratorInstances

  // Type Class instances
  trait ConfiguratorInstances {
    def apply[T](implicit ev: Configurator[T]) = ev

    implicit def localInstance = new Configurator[Local] {
      def getHours(t: Local): Double = t.hours
      def getMongoUrl(t: Local): String = "mongodb://" + t.user + ":" + t.password + "@" + t.ip + "/" + t.database + "." + Constants.collectionIn
      def getHadoopConfig(t: Local): (Configuration,SparkConf) = {
        val conf = new SparkConf().setAppName("LoadToHadoop")
        val config = new Configuration()
        config.set("mongo.input.uri", getMongoUrl(t))
        conf.setMaster("local")
        (config, conf)
      }
      def getSparkSessionBuilder(t: Local): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("LoadToHadoop")
          .config("spark.mongodb.input.uri", getMongoUrl(t))
          .master("local")
        ssBuilder
      }
      def hadoopPersistanceFromHD(rdd: RDD[(Object, org.bson.BSONObject)], t: Local): Unit = {
        rdd.saveAsTextFile(Constants.my_hdfs_fs + "/" + t.hdfsFile)
      }
      def hadoopPersistanceFromSD(rdd: MongoRDD[Document], t: Local): Unit = {
        rdd.map(d => d.toJson).saveAsTextFile(Constants.my_hdfs_fs +"/" + t.hdfsFile)
      }
    }

    implicit def remoteClusterInstance = new Configurator[RemoteCluster] {
      def getHours(t: RemoteCluster): Double = (t.params)(0).toDouble
      def getMongoUrl(t: RemoteCluster): String = "mongodb://" + (t.params)(4) + ":" + (t.params)(5) + "@" + (t.params)(1) + "/" + Constants.database + "." + Constants.collectionIn
      def getHadoopConfig(t: RemoteCluster): (Configuration,SparkConf) = {
        val conf = new SparkConf().setAppName("LoadToHadoop")
        val config = new Configuration()
        config.set("mongo.input.uri", getMongoUrl(t))
        (config, conf)
      }
      def getSparkSessionBuilder(t: RemoteCluster): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("LoadToHadoop")
          .config("spark.mongodb.input.uri", getMongoUrl(t))
        ssBuilder
      }
      def hadoopPersistanceFromHD(rdd: RDD[(Object, org.bson.BSONObject)], t: RemoteCluster): Unit = {
        rdd.saveAsTextFile("hdfs://" + (t.params)(6) + "/" + (t.params)(7))
      }
      def hadoopPersistanceFromSD(rdd: MongoRDD[Document], t: RemoteCluster): Unit = {
        rdd.map(d => d.toJson).saveAsTextFile("hdfs://" + (t.params)(6) + "/" + (t.params)(7))
      }
    }

  }

  // Polymorphical functions
  // Hadoop MongoDB Connector management
  def LoadToHadoopHDFunction[T](configurator: Configurator[T], t: T) = {
    println("=> starting LoadToHadoop application. mode HADOOP")

    val (config: Configuration, conf: SparkConf) = configurator.getHadoopConfig(t)
    val sc = new SparkContext(conf)

    config.set("mongo.job.input.format","com.mongodb.hadoop.MongoInputFormat")

    val keyClassName = classOf[Object]
    val valueClassName = classOf[BSONObject]
    val inputFormatClassName = classOf[com.mongodb.hadoop.MongoInputFormat]
    val ipRDD = sc.newAPIHadoopRDD(config,inputFormatClassName,keyClassName,valueClassName)

    val timeini: Long = System.currentTimeMillis()

    configurator.hadoopPersistanceFromHD(ipRDD, t)

    val timeend: Long = System.currentTimeMillis()

    println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

    sc.stop()
  }

  // Polymorphical functions
  // Spark MongoDB Connector management
  def LoadToHadoopSDFunction[T](configurator: Configurator[T], t: T): Unit = {

    val ssBuilder = configurator.getSparkSessionBuilder(t)
    val ss = ssBuilder.getOrCreate()

    println("=> config loaded!")
    val timeini: Long = System.currentTimeMillis()

    val rddMg: MongoRDD[Document] = MongoSpark.load(ss.sparkContext)
      .withPipeline(Seq(Document.parse("{ $match: { \"date.epoch\" : { $gt : " + (Constants.maxTime - configurator.getHours(t)*Constants.defaultEvalTime) + " } } }")))

    configurator.hadoopPersistanceFromSD(rddMg, t)

    val timeend: Long = System.currentTimeMillis()

    println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

    ss.stop()
  }

  def main(args: Array[String]): Unit = {

      if (args.length == 8) {
        val mode: String = args(7)
        if (mode.equals(Constants.modeHadoopConn)) {
          LoadToHadoopHDFunction[RemoteCluster](Configurator.remoteClusterInstance, RemoteCluster(args))
        } else if (mode.equals(Constants.modeMongoConn)) {
          LoadToHadoopSDFunction[RemoteCluster](Configurator.remoteClusterInstance, RemoteCluster(args))
        } else {
          println("mode param error. " + mode)
        }
      } else if (args.length == 0) {
        // En esta caso local no tengo modo se seleccionar el modo dinamicamente, por lo que se pone lo que se quiera probar
        LoadToHadoopSDFunction[Local](Configurator.localInstance, Local())
      } else {
        println("error in params. " + args)
      }

  }

}
