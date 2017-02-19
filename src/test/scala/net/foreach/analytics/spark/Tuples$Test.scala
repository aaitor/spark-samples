package net.foreach.analytics.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by aitor on 13/2/17.
  */
class Tuples$Test extends FunSuite with BeforeAndAfterAll {


  val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  val sc = new SparkContext(conf)
  val data = List((1, 2), (3, 4), (3, 6))
  val data2 = List((3, 9))
  val rdd = sc.parallelize(data)
  val other = sc.parallelize(data2)

  override def beforeAll() {
  }


  override def afterAll() {
    sc.stop()
  }

  test("testReduce") {

    // Combine values with the same key.
    val reduced = rdd.reduceByKey((x, y) => x + y).sortByKey(true) // {(1, 2), (3, 10)}
    assert(reduced.count() == 2)
    assert(reduced.collect() {
      1
    }._2 == 10)

    // Combine values with the same key.
    val grouped = rdd.groupByKey().sortByKey(true) // {(1, [2]), (3, [4, 6])}
    assert(grouped.count() == 2)
    assert(grouped.collect() {
      1
    }._2.toList {
      0
    } == 4)
    assert(grouped.collect() {
      1
    }._2.toList {
      1
    } == 6)

    // Apply a function to each value of a pair RDD without changing the key.
    val map = rdd.mapValues(x => x * 2).sortByKey(true) // (1, 4), (3 ,8), (3 ,12)}
    assert(map.count() == 3)
    assert(map.collect() {
      0
    }._2 == 4)
    assert(map.collect() {
      1
    }._2 == 8)

    // Apply a function that returns an iterator to each value of a pair RDD,
    // and for each element returned, produce a key/value entry with the old key. Often used for tokenization.
    val flatmap = rdd.flatMapValues(x => x to 4) // {(1,2), (1, 3), (1,4), (3,4)}
    assert(flatmap.count() == 4)

    // Return an RDD of just the keys.
    val keys = rdd.keys // {1, 3, 3}
    assert(keys.collect().toList.equals(List(1, 3, 3)))

    // Return an RDD of just the values.
    val values = rdd.values // {2, 4, 6}
    assert(values.collect().toList.equals(List(2, 4, 6)))

  }

  test("testTransformations") {
    // ((1, 2), (3, 4), (3, 6))  - ((3, 9))
    // Remove elements with a key present in the other RDD.
    val substract = rdd.subtractByKey(other) // {(1, 2)}
    assert(substract.collect().toList {
      0
    }.equals((1, 2)))

    // Perform an inner join between two RDDs
    val join = rdd.join(other) // {(3, (4, 9)), (3, (6, 9))}
    assert(join.collect().toList {
      0
    }._2.equals((4, 9)))
    assert(join.collect().toList {
      1
    }._2.equals((6, 9)))

    // Perform a join between two RDDs where the key must be present in the other RDD
    val rightOuterJoin = rdd.rightOuterJoin(other) // {(3,(Some(4),9)),(3,(Some(6),9))}
    assert(rightOuterJoin.collect().toList {
      0
    }._2.equals((Some(4), 9)))
    assert(rightOuterJoin.collect().toList {
      1
    }._2.equals((Some(6), 9)))

    // Perform a join between two RDDs where the key must be present in the first RDD.
    val leftOuterJoin = rdd.leftOuterJoin(other) // {(1, (2, None)), (3,(4,Some(9))),(3,(6,Some(9)))}
    assert(leftOuterJoin.collect().toList {
      0
    }._2.equals((2, None)))
    assert(leftOuterJoin.collect().toList {
      1
    }._2.equals((4, Some(9))))
    assert(leftOuterJoin.collect().toList {
      2
    }._2.equals((6, Some(9))))

    // Group data from both RDDs sharing the same key.
    val cogroup = rdd.cogroup(other) // {(1,([2],[])), (3, ([4, 6],[9]))}
    assert(cogroup.collect().toList {
      1
    }._2._1.size == 2)
  }

  test("testPairRDD") {
    // ((1, 2), (3, 4), (3, 6))
    // Count the number of elements for each rdd.countByKey() key.
    val countByKey= rdd.countByKey() // {(1, 1), (3, 2)}
    assert(countByKey.equals(Map((1, 1), (3, 2))))

    // Collect the result as a map
    val asMap= rdd.collectAsMap()
    assert(asMap.equals(Map((1, 2), (3, 4), (3, 6))))

    // Return all values associated with the provided key
    val lookup= rdd.lookup(3)
    assert(lookup.equals(Seq(4, 6)))
  }
}