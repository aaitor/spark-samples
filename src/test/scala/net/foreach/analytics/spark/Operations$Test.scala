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

    val tuplesRdd= sc.parallelize(List( (1,"cat"), (2, "mouse"),(3, "cup"), (1, "book"), (4, "tv"), (5, "screen"), (2, "heater")))
    val reduced= tuplesRdd.reduceByKey((x, y) => x.concat("-" + y))
    assert( reduced.count() == 5)
    val tupleSorted= reduced.collect().sortBy(_._1)

    assert( tupleSorted{0}._2.equals("cat-book") )
    assert( tupleSorted{1}._2.equals("mouse-heater") )
    assert( tupleSorted{2}._2.equals("cup") )


  }

  test("testAggregate") {
    val numbers= sc.parallelize(1 to 5 toList)

    val zeroVal= -1
    val agg= numbers.aggregate((zeroVal))(
      (acc, value) => (acc + value),
      (acc1, acc2) => (acc1 + acc2)
    )
    assert(agg == (1+2+3+4+5 + (zeroVal + zeroVal)))
  }

}
