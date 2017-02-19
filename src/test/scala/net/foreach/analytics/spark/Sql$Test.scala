package net.foreach.analytics.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by aitor on 13/2/17.
  */
class Sql$Test extends FunSuite with BeforeAndAfterAll {


  val conf= new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  val sc= new SparkContext(conf)
  //val sql= new SQLContext(sc)
  val session= SparkSession.builder()
  session.config(conf)
  val sql= session.getOrCreate().sqlContext



  override def beforeAll() {
  }


  override def afterAll() {
    sc.stop()
  }

  test("testTweets") {
    val input= sql.read.json("src/test/resources/tweets.json")
    input.createOrReplaceTempView("tweets")
    sql.cacheTable("tweets")
    val topTweets= sql.sql("SELECT text, retweet_count FROM tweets ORDER BY retweet_count DESC LIMIT 10")

    topTweets.show(1)

    val result= topTweets.collect()

    assert(result.size == 10)
    assert(result{0}.getAs[Long]("retweet_count") == 173L)

    assert(topTweets.filter("retweet_count > 25").count() == 2)

    val groupById= sql.sql("SELECT user.id as userId, COUNT(*) as count FROM tweets " +
      "GROUP BY user.id ORDER BY count DESC  LIMIT 5")
    groupById.show(5)
    val resultGroup= groupById.collect()
    assert(resultGroup.size == 5)
    assert(resultGroup{0}.getAs[Long]("userId") == 14498554L)
    assert(resultGroup{0}.getAs[Long]("count") == 2L)


  }


}