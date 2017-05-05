/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.lu.location

import java.text.SimpleDateFormat
import java.util.Locale

import scala.Iterator
import scala.Left
import scala.Right
import scala.reflect.ClassTag

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.json.JSONObject

import com.esri.core.geometry.Point
import com.lu.taxi.FeatureCollection

import com.lu.taxi.GeoJsonProtocol._
import spray.json._
import com.lu.taxi.Feature

object LocationAnalysis extends Serializable {

  val resultSchema =
    StructType(
      StructField("id", StringType, true) ::
        StructField("country", StringType, true) ::
        StructField("time", StringType, true) ::
        StructField("location", StringType, true) :: Nil)

  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)

  def main(args: Array[String]): Unit = {
    //params hiveTable tableOneSavePath tableTwoSavePath
    val conf = new SparkConf().setAppName("LocationAnalysis").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //sample  {"id":"123","country":"chaoxin","time":"2017-05-02 12:12:00","location":"50,110"}
    val sqlContext = new SQLContext(sc)
    val locationDF = sqlContext.read.json("file:///Users/liujl/test/lu_location.json")
    //HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(ssc.sparkContext)
    //val locationDF = sqlContext.sql("select * from xxx.xxx")

    val geojson = scala.io.Source.fromURL(getClass.getResource("/nyc-boroughs.geojson")).mkString
    val features = geojson.parseJson.convertTo[FeatureCollection]
    val broadFeatures = sc.broadcast(features)

    println("-----------")
    locationDF.foreach { row => print(row) }
    features.foreach { row => print(row) }
    println("-----------")

    val outputJSONRDD = locationDF.toJSON.map { json =>
      val jsonObject = new JSONObject(json)
      val location = jsonObject.getString("location").split(",")
      val geo = point(location(0), location(1))
      val findResult: Option[Feature] = broadFeatures.value.find(feature => {
        feature.geometry.contains(geo) //在禁入国家出现
      })
      if (findResult.isEmpty) { //不在禁入名单
        json //todo测试用  不在名单代码用   ""
      } else {
        json
      }
    }.filter { x => !x.equals("") }
    val reader = sqlContext.read
    reader.schema(resultSchema)
    val outputDF = reader.json(outputJSONRDD)
    println("-----result------")
    outputDF.foreach { row => print(row) }
    //outputDF.write.mode(SaveMode.Append).parquet(savePath)
    //output table1: id  country time location
    //output table2: id  country 次数（每个人出现的次数）

  }

  def point(longitude: String, latitude: String): Point = {
    new Point(longitude.toDouble, latitude.toDouble)
  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }

}

