package es.bernal.sparkmongoiot

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import es.bernal.sparkmongoiot.utils.Constants
import es.bernal.sparkmongoiot.types._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.bson.Document

import scala.collection.mutable.ListBuffer

/**
  * Created by bernal on 25/4/17.
  */
object Analytic {

  // Basic parameter classes
  trait Environment // T classes
  class Home(
                    val ip: String = Constants.ip,
                    val database: String = Constants.database,
                    val outputCollection: String = Constants.collectionOutAgg
                  ) extends Environment
  class Local(
                    val user: String = Constants.user,
                    val password: String = Constants.password
                  ) extends Home
  class RemoteCluster(val params: Array[String]) extends Environment

  trait DataLoader // V classes
  class MongoDbLoader(
                           val hours: Double = 234.5256
                         ) extends DataLoader
  class HadoopLoader(
                        val hdfsHostAndPort: String = Constants.hdfsHostPort
                        ) extends DataLoader


  // Type Class interfaces
  trait Configurator[T] {
    def getSparkSessionBuilder(t: T): SparkSession.Builder
    def getHadoopRdd(t: T, sparkSession: SparkSession): RDD[String]
  }

  trait OriginDataHandler[T,V] {
    def getBaseDataFrame(c: Configurator[T], t: T, v: V, sparkSession: SparkSession): DataFrame
  }

  object Configurator extends ConfiguratorInstances
  object OriginDataHandler extends OriginDataHandlerInstances

  // Type Class instances
  trait ConfiguratorInstances {
    def apply[T](implicit ev: Configurator[T]) = ev

    implicit def homeInstance = new Configurator[Home] {
      def getSparkSessionBuilder(t: Home): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("spark-mongo-iot")
          .master("local[*]")
          .config("spark.mongodb.input.uri", "mongodb://" + t.ip + "/"+ t.database + "." + Constants.collectionIn)
          .config("spark.mongodb.output.uri", "mongodb://" + t.ip + "/"+ t.database + "." + t.outputCollection)
        ssBuilder
      }
      def getHadoopRdd(t: Home, sparkSession: SparkSession): RDD[String] = {
        sparkSession.sparkContext.textFile(Constants.my_hdfs_fs + "/" + Constants.hdfsFile)
      }
    }

    implicit def localInstance = new Configurator[Local] {
      def getSparkSessionBuilder(t: Local): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("spark-mongo-iot")
          .master("local[*]")
          .config("spark.mongodb.input.uri", "mongodb://" + t.user + ":" + t.password + "@" + t.ip + "/" + t.database + "." + Constants.collectionIn)
          .config("spark.mongodb.output.uri", "mongodb://" + t.user + ":" + t.password + "@" + t.ip + "/" + t.database + "." + t.outputCollection)
        ssBuilder
      }
      def getHadoopRdd(t: Local, sparkSession: SparkSession): RDD[String] = {
        sparkSession.sparkContext.textFile(Constants.my_hdfs_fs + "/" + Constants.hdfsFile)
      }
    }

    implicit def remoteClusterInstance = new Configurator[RemoteCluster] {
      def getSparkSessionBuilder(t: RemoteCluster): SparkSession.Builder = {
        val ssBuilder = SparkSession.builder()
          .appName("spark-mongo-iot")
          .config("spark.mongodb.input.uri", "mongodb://" + t.params(4) + ":" + t.params(5) + "@" + t.params(1) + "/" + t.params(2) + "." + Constants.collectionIn)
          .config("spark.mongodb.output.uri", "mongodb://" + t.params(4) + ":" + t.params(5) + "@" + t.params(1) + "/" + t.params(2) + "." + t.params(3))
        ssBuilder
      }
      def getHadoopRdd(t: RemoteCluster, sparkSession: SparkSession): RDD[String] = {
        sparkSession.sparkContext.textFile("hdfs://" + t.params(6) + "/" + Constants.hdfsFile)
      }
    }

  }

  // Type Class instances
  trait OriginDataHandlerInstances {
    def apply[T,V](implicit ev: OriginDataHandler[T,V]) = ev

    // Hadoop solution
    implicit def homeByHadoopInstance = new OriginDataHandler[Home, HadoopLoader] {
      def getBaseDataFrame(c: Configurator[Home], t: Home, v: HadoopLoader, sparkSession: SparkSession): DataFrame = {
        getHadoopDataFrame(c, t, sparkSession)
      }
    }
    implicit def localByHadoopInstance = new OriginDataHandler[Local, HadoopLoader] {
      def getBaseDataFrame(c: Configurator[Local], t: Local, v: HadoopLoader, sparkSession: SparkSession): DataFrame = {
        getHadoopDataFrame(c, t, sparkSession)
      }
    }
    implicit def remoteClusterByHadoopInstance = new OriginDataHandler[RemoteCluster, HadoopLoader] {
      def getBaseDataFrame(c: Configurator[RemoteCluster], t: RemoteCluster, v: HadoopLoader, sparkSession: SparkSession): DataFrame = {
        getHadoopDataFrame(c, t, sparkSession)
      }
    }
    def getHadoopDataFrame[T](c: Configurator[T], t: T, sparkSession: SparkSession): DataFrame = {
      val hadoopRdd: RDD[String] = c.getHadoopRdd(t, sparkSession)
      sparkSession.read.json(hadoopRdd)
    }

    // MongoDb Solution
    implicit def homeBymongoDbInstance = new OriginDataHandler[Home, MongoDbLoader] {
      def getBaseDataFrame(c: Configurator[Home], t: Home, v: MongoDbLoader, sparkSession: SparkSession): DataFrame = {
        getMongoDbBaseDataFrame(v, sparkSession)
      }
    }
    implicit def localBymongoDbInstance = new OriginDataHandler[Local, MongoDbLoader] {
      def getBaseDataFrame(c: Configurator[Local], t: Local, v: MongoDbLoader, sparkSession: SparkSession): DataFrame = {
        getMongoDbBaseDataFrame(v, sparkSession)
      }
    }
    implicit def remoteClusterBymongoDbInstance = new OriginDataHandler[RemoteCluster, MongoDbLoader] {
      def getBaseDataFrame(c: Configurator[RemoteCluster], t: RemoteCluster, v: MongoDbLoader, sparkSession: SparkSession): DataFrame = {
        getMongoDbBaseDataFrame(v, sparkSession)
      }
    }
    def getMongoDbBaseDataFrame(v: MongoDbLoader, sparkSession: SparkSession): DataFrame = {
      // Load Data from Mongo by Spark MongoDB Connector

      //  val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(ss)))
      //  val rdd = MongoSpark.load(ss, readConfig)
      import sparkSession.sqlContext.implicits._
      val rddMg: MongoRDD[Document] = MongoSpark.load(sparkSession.sparkContext)
        .withPipeline(Seq(Document.parse("{ $match: { \"date.epoch\" : { $gt : " + (Constants.maxTime - v.hours*Constants.defaultEvalTime) + " } } }")))
      rddMg.toDF()
    }

  }

  // Polymorphical function
  def analyticFunction[T,V](configurator: Configurator[T], originDataHandler: OriginDataHandler[T,V], t: T, v: V): Unit = {

    println("=> starting spark-mongo-iot sample")

    val ssBuilder = configurator.getSparkSessionBuilder(t)
    val ss = ssBuilder.getOrCreate()
    import ss.sqlContext.implicits._

    println("=> config loaded!")
    val timeini: Long = System.currentTimeMillis()

    var rdd: DataFrame = originDataHandler.getBaseDataFrame(configurator, t, v, ss);

    // From here, work with DataFrame

    val rddProjected = rdd.select(col("organizationId"),col("channelId"),
      col("datastreamId"), col("deviceId"), col("date.epoch").as("date_epoch"),
      col("value")).cache

    // Extract list of datastreams
    val datastreams = rddProjected.select("datastreamId").distinct()

    val it = datastreams.toLocalIterator()
    val dsList = new ListBuffer[String]()
    while (it.hasNext) {
      val ds = it.next().getAs[String]("datastreamId")
      dsList += ds
    }

    val allDs = dsList.toList.par

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

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      // home or local case, depending on what to test (mongo load by default)
      analyticFunction(Configurator.homeInstance, OriginDataHandler.homeBymongoDbInstance, new Home(), new MongoDbLoader())
    } else if (args.length == 7) {
      if (args(6).equals(Constants.modeMongoConn)) {
        // RemoteCluster Mongo originated data
        analyticFunction(Configurator.remoteClusterInstance, OriginDataHandler.remoteClusterBymongoDbInstance, new RemoteCluster(args), new MongoDbLoader())
      } else {
        // RemoteCluster Hadoop originated data
        analyticFunction(Configurator.remoteClusterInstance, OriginDataHandler.remoteClusterByHadoopInstance, new RemoteCluster(args), new HadoopLoader(args(6)))
      }
    }
  }

}
