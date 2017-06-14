package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.utils.Constants
import org.apache.spark.sql.SparkSession
import org.bson.Document

/**
  * Created by bernal on 25/4/17.
  */
object SimpleMongoProcess {

  // Basic parameter classes
  trait Environment
  case class Home(
                    val hours: Double = 234.5256,
                    val ip: String = Constants.ip,
                    val database: String = Constants.database,
                    val outputCollection: String = Constants.collectionOutAgg
                  ) extends Environment
  case class Local(
                    val hours: Double = 234.5256,
                    val ip: String = Constants.ip,
                    val database: String = Constants.database,
                    val outputCollection: String = Constants.collectionOutAgg,
                    val user: String = Constants.user,
                    val password: String = Constants.password
                  ) extends Environment
  abstract class Cluster extends Environment {
     def params: Array[String]
  }
  case class LocalCluster(val params: Array[String]) extends Cluster
  case class RemoteCluster(val params: Array[String]) extends Cluster

  // Type Class interface
  trait Configurator[T] {
    def getHours(t: T): Double
    def getSparkSessionBuilder(t: T): SparkSession.Builder
  }

  object Configurator extends ConfiguratorInstances

  // Type Class instances
  trait ConfiguratorInstances {
    def apply[T](implicit ev: Configurator[T]) = ev

    implicit def homeInstance = new Configurator[Home] {
      def getHours(t: Home): Double = t.hours
      def getSparkSessionBuilder(t: Home): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("SampleMongoProcess")
          .master("local[*]")
          .config("spark.mongodb.input.uri", "mongodb://" + t.ip + "/" + t.database + "." + Constants.collectionIn)
          .config("spark.mongodb.output.uri", "mongodb://" + t.ip + "/" + t.database + "." + t.outputCollection)
        ssBuilder
      }
    }

    implicit def localInstance = new Configurator[Local] {
      def getHours(t: Local): Double = t.hours
      def getSparkSessionBuilder(t: Local): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("SampleMongoProcess")
          .master("local[*]")
          .config("spark.mongodb.input.uri", "mongodb://" + t.user + ":" + t.password + "@" + t.ip + "/" + t.database + "." + Constants.collectionIn)
          .config("spark.mongodb.output.uri", "mongodb://" + t.user + ":" + t.password + "@" + t.ip + "/" + t.database + "." + t.outputCollection)
        ssBuilder
      }
    }

    implicit def localClusterInstance = new Configurator[LocalCluster] {
      def getHours(t: LocalCluster): Double = (t.params)(0).toDouble
      def getSparkSessionBuilder(t: LocalCluster): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("SampleMongoProcess")
          .config("spark.mongodb.input.uri", "mongodb://" + (t.params)(1) + "/"+ (t.params)(2) + "." + Constants.collectionIn)
          .config("spark.mongodb.output.uri", "mongodb://" + (t.params)(1) + "/"+ (t.params)(3) + "." + (t.params)(3))
        ssBuilder
      }
    }

    implicit def remoteClusterInstance = new Configurator[RemoteCluster] {
      def getHours(t: RemoteCluster): Double = (t.params)(0).toDouble
      def getSparkSessionBuilder(t: RemoteCluster): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("SampleMongoProcess")
          .config("spark.mongodb.input.uri", "mongodb://" + (t.params)(4) + ":" + (t.params)(5) + "@" + (t.params)(1) + "/" + (t.params)(2) + "." + Constants.collectionIn)
          .config("spark.mongodb.output.uri", "mongodb://" + (t.params)(4) + ":" + (t.params)(5) + "@" + (t.params)(1) + "/" + (t.params)(2) + "." + (t.params)(3))
        ssBuilder
      }
    }

  }

  // Polymorphic function
  def SimpleMongoProcessFunction[T](configurator: Configurator[T], t: T): Unit = {
    println("=> starting Sample Mongo Process")
    val ssBuilder = configurator.getSparkSessionBuilder(t)

    val ss = ssBuilder.getOrCreate()
    import ss.sqlContext.implicits._

    println("=> config loaded!")
    val timeini: Long = System.currentTimeMillis()

    val rddMg: MongoRDD[Document] = MongoSpark.load(ss.sparkContext)
      .withPipeline(Seq(Document.parse("{ $match: { \"date.epoch\" : { $gt : " + (Constants.maxTime - configurator.getHours(t)*Constants.defaultEvalTime) + " } } }")))

    MongoSpark.save(rddMg)

    val timeend: Long = System.currentTimeMillis()

    println("\nTotal time " + (timeend - timeini)/1000 + " s\n")

    ss.stop()


  }

  // Main application
  def main(args: Array[String]): Unit = {

      if (args.size == 0) {
        SimpleMongoProcessFunction[Home](Configurator.homeInstance, Home())
      } else if (args.size == 6) {
        SimpleMongoProcessFunction[RemoteCluster](Configurator.remoteClusterInstance, RemoteCluster(args))
      } else if (args.size == 4) {
        SimpleMongoProcessFunction[LocalCluster](Configurator.localClusterInstance, LocalCluster(args))
      } else {
        throw new Throwable("invalid params")
      }

  }

}
