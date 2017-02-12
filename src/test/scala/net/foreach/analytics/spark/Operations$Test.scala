package net.foreach.analytics.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by aitor on 12/2/17.
  */
class Operations$Test extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  val sc = new SparkContext(conf)

  override def beforeAll() {
  }


  override def afterAll() {
    sc.stop()
  }

  test("testCount") {
    val numbers= sc.parallelize(1 to 10 toList)

    assert(numbers.count() == 10L)

    val union= numbers.union(sc.parallelize(8 to 12))
    val countByValue= union.countByValue()

    assert(countByValue.get(1).get == 1L)
    assert(countByValue.get(8).get == 2L)
    assert(countByValue.size == 12L)
  }

  test("testReduce") {
    val numbers= sc.parallelize(1 to 5 toList)
    val numbersB= sc.parallelize(List()).asInstanceOf[RDD[Int]]

    assert( numbers.reduce((x,y) => x+y) == 15)
    assertThrows[UnsupportedOperationException] {
      (numbersB.reduce((x,y) => x+y))
    }

    assert( numbers.fold(0)((x,y) => x+y) == 15)
    assert( numbersB.fold(0)((x,y) => x+y) == 0)
  }


}
