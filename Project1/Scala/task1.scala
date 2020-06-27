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


import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.classTag

object task1 {

  def clean_data(word:String, stopwords: ListBuffer[String]): String = {
    //println(word.toString)
    val pun = List('(', '[', ',', '.', '!', '?', ':', ';', ']', ')')
    var w=""
    if (!(stopwords contains word)) {
      //println("here")
      for (c <- word) {
        if  (!(pun contains c)) {
          //println("here22")
          //println(c)
          w = w + c.toString()

        }
      }
    }
    //println("Hello")
    return(w)
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession.builder().appName("task1").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext

    val jsonRDD = sc.textFile(args(0))
    jsonRDD.persist()
    //val stp_words = sc.textFile("/Users/akshatasheth/Desktop/Data Mining/Assignment 1/stopwords")
    var stopwords = new ListBuffer[String]()
    for (line <- Source.fromFile("/Users/akshatasheth/Desktop/Data Mining/Assignment 1/stopwords").getLines) {
      stopwords += line.trim()
    }
    println(stopwords)


    val total_users = jsonRDD.count()
    val num_users = jsonRDD.filter(row => compact(parse(row) \ "date").replace("\"", "").split("-")(0) == args(2).toInt).count()
    val distinct_users = jsonRDD.map{row => compact(parse(row) \ "user_id")}.distinct().count()
    val top10_popular_names = jsonRDD.map{row => (compact(parse(row) \ "user_id"), 1)}.reduceByKey(_+_).sortBy(_._2, ascending=false).take(args(3).toInt)
    val anser4 = jsonRDD.flatMap{row => (compact(parse(row) \ "text")).toLowerCase().split(' ')}
    var new12 = anser4.map{x => (clean_data(x, stopwords),1)}.filter(x => (x._1 != "") && (x._1 != None)).reduceByKey(_+_).sortBy(_._2, ascending=false).take(args(4).toInt)
    var result = new ListBuffer[String]()
    for (x<-new12){
      result += x._1

    }
    println(new12.toList)

    val task1 = mutable.LinkedHashMap(
      "A" -> total_users,
      "B" -> num_users,
      "C" -> distinct_users,
      "D" -> top10_popular_names,
      "E" -> result)

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val file = new File("output.json")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(mapper.writeValueAsString(task1).replaceAll("\\\\\"", ""))
    bw.close()
  }

}
