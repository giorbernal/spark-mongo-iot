package es.bernal.sparkmongoiot

import java.io.File
import java.net.URLClassLoader

import com.google.gson.Gson
import com.mongodb.spark.MongoSpark
import es.bernal.sparkmongoiot.types.{DataPointCnt, DataPointDct, DataPoint, DsTime}
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

  ss.close()

  def getFiles(): List[(String,String)] = {
    var files = new ListBuffer[(String)]()
    var typeFilesMap = new ListBuffer[(String,String)]()

    val path = getClass.getResource("/data")
    val folder = new File(path.getPath)
    if (folder.exists && folder.isDirectory)
      folder.listFiles
        .toList
        .filter(f => !f.getName.toLowerCase.contains(Constants.GPRS_SESSION))
        .foreach(file => files+=file.toURI.getPath)

    files.foreach(file => typeFilesMap += Tuple2(getTypeOfFile(file),file))

    // TODO Loop to read all files
    // Test Case
   // typeFilesMap += Tuple2(Constants.PRESENCE,new File(this.getClass.getClassLoader.getResource("data/Presence_2011_02_04.csv").toURI).getPath)

    typeFilesMap.toList
  }

  // DataPoints Parser
  def parseDataPoint(datastream: String, line: String): DataPoint = {
    var a: Array[String] = Array()
    var b: Array[String] = Array()
    a = line.split(';')

    datastream match{
      case Constants.PRESENCE =>
        b = a(5).split('|')
        return DataPointDct(null, null, datastream, a(1), Constants.ORG, a(0), DsTime(b(1).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, b(0))

      case Constants.STATUS =>
        a = line.split(';')
        b = a(4).split('|')
        return DataPointCnt(null, null, Constants.COVERAGE, a(1), Constants.ORG, a(0), DsTime(b(1).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, b(0).toDouble)

      case Constants.INVENTORY_ICC =>
        a = line.split(';')
        b = a(4).split('|')
        return DataPointDct(null, null, datastream, a(1), Constants.ORG, a(0), DsTime(b(1).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, b(0))

      case Constants.INVENTORY_IMEI =>
        a = line.split(';')
        b = a(4).split('|')
        return DataPointDct(null, null, datastream, a(1), Constants.ORG, a(0), DsTime(b(1).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, b(0))

      case Constants.INVENTORY_IMSI =>
        a = line.split(';')
        b = a(4).split('|')
        return DataPointDct(null, null, datastream, a(1), Constants.ORG, a(0), DsTime(b(1).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, b(0))

      case Constants.INVENTORY_MANUFACTURER =>
        a = line.split(';')
        b = a(4).split('|')
        return DataPointDct(null, null, datastream, a(1), Constants.ORG, a(0), DsTime(b(2).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, b(0))

      case Constants.INVENTORY_FIRMWARE =>
        a = line.split(';')
        b = a(4).split('|')
        return DataPointDct(null, null, datastream, a(1), Constants.ORG, a(0), DsTime(b(1).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, b(0))

      case Constants.LOCATION =>
        a = line.split(';')
        b = a(4).split('|')
        return DataPointDct(null, null, datastream, a(1), Constants.ORG, a(0), DsTime(b(1).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), null, b(0))
      case default => return null
    }

  }

  def getTypeOfFile(filename: String): String ={
    if(filename.toLowerCase().contains(Constants.PRESENCE)){
      return Constants.PRESENCE
    }
    else if(filename.toLowerCase().contains(Constants.INVENTORY)){
      if(filename.toLowerCase().contains(Constants.INVENTORY_IMEI)){
        return Constants.INVENTORY_IMEI
      }
      else if(filename.toLowerCase().contains(Constants.INVENTORY_IMSI)){
        return Constants.INVENTORY_IMSI
      }
      else if(filename.toLowerCase().contains(Constants.INVENTORY_MANUFACTURER)){
        return Constants.INVENTORY_MANUFACTURER
      }
      else if(filename.toLowerCase().contains(Constants.INVENTORY_ICC)){
        return Constants.INVENTORY_ICC
      }
      else return Constants.UNDEFINED
    }
    else if(filename.toLowerCase().contains(Constants.STATUS)){
      return Constants.STATUS
    }
    else if(filename.toLowerCase().contains(Constants.LOCATION)){
      return Constants.LOCATION
    }
    else{
      return Constants.UNDEFINED
    }

  }



}
