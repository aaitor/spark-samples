package net.foreach.analytics.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aitor on 12/2/17.
  */
object WordCount {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val input= sc.textFile("src/main/resources/some-text.txt")

    val words= input.flatMap(line => line.split(" "))
    val counts= words
      .map(word => (word.toLowerCase, 1))
      .reduceByKey((x,y) => x+y )
      .sortBy(_._2, false)

    counts.take(10).foreach(println)
    //counts.foreach(println)

    sc.stop();

  }



}
