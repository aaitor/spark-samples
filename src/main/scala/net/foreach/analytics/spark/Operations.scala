package net.foreach.analytics.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aitor on 12/2/17.
  */
object Operations {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    //val input= sc.textFile("src/main/resources/some-text.txt")

    val results= sc.parallelize(List(1,2,3,4,5,6,7,8))



    sc.stop();

  }



}
