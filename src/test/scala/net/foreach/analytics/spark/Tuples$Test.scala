package net.foreach.analytics.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by aitor on 13/2/17.
  */
class Tuples$Test extends FunSuite with BeforeAndAfterAll {


  val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  val sc = new SparkContext(conf)
  val data= List((1, 2), (3, 4), (3, 6))
  val data2= List((3, 9))
  val rdd= sc.parallelize(data)
  val other= sc.parallelize(data2)

  override def beforeAll() {
  }


  override def afterAll() {
    sc.stop()
  }

  test("testReduce") {

    // Combine values with the same key.
    val reduced= rdd.reduceByKey((x, y) => x +y).sortByKey(true) // {(1, 2), (3, 10)}
    assert(reduced.count() == 2)
    assert(reduced.collect(){1}._2 == 10)

    // Combine values with the same key.
    val grouped= rdd.groupByKey().sortByKey(true) // {(1, [2]), (3, [4, 6])}
    assert(grouped.count() == 2)
    assert(grouped.collect(){1}._2.toList{0} == 4)
    assert(grouped.collect(){1}._2.toList{1} == 6)

    // Apply a function to each value of a pair RDD without changing the key.
    val map= rdd.mapValues(x => x*2).sortByKey(true) // (1, 4), (3 ,8), (3 ,12)}
    assert(map.count() == 3)
    assert(map.collect(){0}._2 == 4)
    assert(map.collect(){1}._2 == 8)

    // Apply a function that returns an iterator to each value of a pair RDD,
    // and for each element returned, produce a key/value entry with the old key. Often used for tokenization.
    val flatmap= rdd.flatMapValues(x => x to 4) // {(1,2), (1, 3), (1,4), (3,4)}
    assert(flatmap.count() == 4)

    // Return an RDD of just the keys.
    val keys= rdd.keys // {1, 3, 3}
    assert( keys.collect().toList.equals(List(1,3,3)))

    // Return an RDD of just the values.
    val values= rdd.values // {2, 4, 6}
    assert( values.collect().toList.equals(List(2,4,6)))

  }

  test("testTransformations") {
    // Remove elements with a key present in the other RDD.
    val substract= rdd.subtractByKey(other) // {(1, 2)}
    assert( substract.collect().toList{0}.equals((1,2)))

    // Perform an inner join between two RDDs
    val join= rdd.join(other) // {(3, (4, 9)), (3, (6, 9))}
    assert( join.collect().toList{0}._2.equals( (4,9) ))
    assert( join.collect().toList{1}._2.equals( (6,9) ))




  }
}
