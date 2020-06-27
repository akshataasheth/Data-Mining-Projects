import java.io.{BufferedWriter, File, FileWriter}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods.{compact, parse}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function2

import scala.io.Source
import org.json4s._
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonDSL._

import scala.util.parsing.json.JSON
//import org.json4s.native.JsonMethods._


import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.classTag




object task2 {
  implicit val formats = DefaultFormats
  def splitValues(values: String): ListBuffer[String] = {
    var values1 = values.drop(1).dropRight(1)
    //println(values1.toList)
    var words = new ListBuffer[String]()
    for (value <- values1.split(',')) {
      words += value.trim()
    }
    return words

  }



  def main(args: Array[String]): Unit = {
    implicit val formats = DefaultFormats
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession.builder().appName("task1").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext

    var reviewRDD = sc.textFile("/Users/akshatasheth/Desktop/Data Mining/Assignment 1/review.json")
    var businessRDD = sc.textFile("/Users/akshatasheth/Desktop/Data Mining/Assignment 1/business.json")

    //Process review data first
    var reviewRDD1 = reviewRDD.map { review => (compact(parse(review) \ ("business_id")) , compact(parse(review) \ "stars") )}.mapValues(value => value.toFloat)
    //println(reviewRDD1.toList)
    var sumRDD = reviewRDD1.groupByKey().map { x => (x._1, (x._2.sum, x._2.size)) }
    //println(sumRDD.toList)
    //Process business data first
    var businessRDD1 = businessRDD.map { review => (compact(parse(review) \ ("business_id")) , compact(parse(review) \ "categories") ) }
    //println(businessRDD1.toList)

    var filteredRDD = businessRDD1.filter(x => (x._1 != null) && (x._1 != "")).mapValues(values => splitValues(values.toString))
    //println(filteredRDD.toList)


    var leftOuterJoinRDD = filteredRDD.join(sumRDD)
    //.map {case (a, (b, c: Option[(Float,Int)])) => (a, (b, (c.getOrElse()))) }
    //println(leftOuterJoinRDD.toList)

    //Calculate average for each category
    //extract the values as a new pair (category,(sum(stars),len(stars)))
    val averageRDD = leftOuterJoinRDD.map{x => x._2}.filter(x => (x._2 != null && x._2 != "")).flatMap(x => {for (category <- x._1) yield {(category, x._2)}})
    //println(averageRDD.toList)
    //println(averageRDD)
    val averageRDD1 = averageRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues( x => (x._1/x._2)).sortBy(x => (x._2, x._1))(Ordering.Tuple2(Ordering.Float.reverse,Ordering.String), classTag[(Float,String)]).take(10)
    //.sortBy(_._2).take(10)
    //println(averageRDD1.toList)

    val task2 = mutable.LinkedHashMap(
      "results" -> averageRDD1)

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val file = new File("output2.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(mapper.writeValueAsString(task2).replaceAll("\\\\\"", ""))
    bw.close()
  }
}
