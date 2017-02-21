package net.foreach.analytics.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.{SQLImplicits, SparkSession}

/**
  * Created by aitor on 13/2/17.
  */
class HbaseIT extends FunSuite with BeforeAndAfterAll {

  val tableName= "analytics_demo"
  val conf= new SparkConf().setAppName(this.getClass.getName).setMaster("local")
  val sc= new SparkContext(conf)

  val session= SparkSession.builder()
  session.config(conf)
  val sql= session.getOrCreate().sqlContext

  val hbaseConf= HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.quorum", "192.168.0.29");
  hbaseConf.set("hbase.zookeeper.property.clientPort","2181");
  //hbaseConf.addResource("src/test/resources/hadoop/core-site.xml")
  //hbaseConf.addResource("src/test/resources/hadoop/hbase-site.xml")
  hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)



  override def beforeAll() {
  }


  override def afterAll() {
    sc.stop()
  }

  test("testReadFromDB") {
    val hbaseRDD= sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    //val count = hbaseRDD.count()

    val beanRDD= hbaseRDD.map(kv => (kv._1.get(), AnalyticsDemo.navMapToMap(kv._2.getMap)))
    val df= sql.createDataFrame(beanRDD, classOf[AnalyticsDemo])
    assert(df.count() == 49L)

    //df.createOrReplaceTempView(tableName)
    //val dfResult= sql.sql("SELECT day, hour, total FROM " + tableName + " LIMIT 5")
    //dfResult.show(3)

    //assert( dfResult.count() == 5L)
   }

}