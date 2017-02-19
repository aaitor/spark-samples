package net.foreach.analytics.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by aitor on 13/2/17.
  */
class Streaming$Test extends FunSuite with BeforeAndAfterAll {


  val conf= new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  val sc= new SparkContext(conf)




  override def beforeAll() {
  }


  override def afterAll() {
    sc.stop()
  }

  test("testStreaming") {
    //val ssc= new StreamingContext(conf, Seconds(1))
    //ssc.socketTextStream("localhost", 7777)



    //ssc.stop()
  }


}