import java.io.{BufferedWriter, File, FileWriter}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.RDDConversions._
import org.apache.spark.rdd.PairRDDFunctions
import org.json4s.jackson.JsonMethods.{compact, parse}

import scala.collection.mutable

    object task3{
      class custom[V](numParts: Int, elements: Long) extends Partitioner {
        override def numPartitions: Int = numParts

        override def getPartition(key: Any): Int = {
          var k = key.toString.charAt(1)
          return (k.hashCode()%numParts)
        }
      }

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      val ss = SparkSession.builder().appName("task1").config("spark.master", "local[*]").getOrCreate()
      val sc = ss.sparkContext
      var spark = "cus"
      var n = 20

      var jsonRDD = sc.textFile("/Users/akshatasheth/Desktop/Data Mining/Assignment 1/review.json")
      var reviewRDD = jsonRDD.map { row => (compact(parse(row) \ "business_id"), 1) }
      var finalRDD = reviewRDD

      if (spark != "Default") {
        var finalRDD = reviewRDD.partitionBy(new custom(20, reviewRDD.count()))

      }

      var num_items = finalRDD.mapPartitions(iter => Array(iter.size).iterator, true).collect()
      var results = finalRDD.reduceByKey(_+_).filter( x => x._2 > n).collect()

      val task2 = mutable.LinkedHashMap(
        "n_partition" -> num_items.length,
        "n_items" -> num_items,
      "result" -> results)

      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      val file = new File("output2.json")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(mapper.writeValueAsString(task2).replaceAll("\\\\\"", ""))
      bw.close()
      //println(m1.toList)



    }
  }

