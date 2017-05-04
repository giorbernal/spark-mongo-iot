package es.bernal.sparkmongoiot

import java.io.File

import com.google.gson.Gson
import com.mongodb.spark.MongoSpark
import es.bernal.sparkmongoiot.types.{DataPoint, DsTime}
import es.bernal.sparkmongoiot.utils.Constants
import org.apache.spark.rdd.RDD
//import net.liftweb.json._
//import net.liftweb.json.Serialization.write
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.collection.mutable.ListBuffer

/**
  * Created by bernal on 4/5/17.
  */
object DataLoader extends App {

  println("DataLoader application")

  val ss = SparkSession.builder()
    .master("local")
    .appName("DataLoader")
    .config("spark.mongodb.output.uri", "mongodb://" + Constants.ip + "/"+ Constants.database + "." + Constants.collectionOut)
    .getOrCreate()


  def files: List[(String,String)] = getFiles

  files.foreach(f => {
    // Load Data
    val dsRDD: RDD[DataPoint] = ss.sparkContext.textFile(f._2).map(a => parseDataPoint(f._1,a))
    // extracción de documentos
    val dsDocs: RDD[Document] = dsRDD
      .map(dp => {
//        implicit val formats = DefaultFormats
//        val jsonStr = write(dp)
        val gson = new Gson
        val jsonStr = gson.toJson(dp)
        jsonStr
      })
      .map( i => Document.parse(i) )

    // Volcar a Mongo
    MongoSpark.save(dsDocs)

  })

  def getFiles(): List[(String,String)] = {
    var files = new ListBuffer[(String,String)]()

    // TODO Loop to read all files
    // Test Case
    files += Tuple2(Constants.PRESENCE,new File(this.getClass.getClassLoader.getResource("data/Presence_2011_02_04.csv").toURI).getPath)

    files.toList
  }

  // DataPoints Parser
  def parseDataPoint(datastream: String, line: String): DataPoint = {
    val a: Array[String] = line.split(";")

    if ( datastream.equals(Constants.PRESENCE) ) {
      return DataPoint(null, null, datastream, a(1), Constants.ORG, a(0), DsTime(a(8).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, a(7))
    }
    // TODO resto de modelos de datastreams
    ???
  }

}
