package net.foreach.analytics.spark

import scala.collection.JavaConverters._

/**
  * Created by aitor
  */
@SerialVersionUID(100L)
case class AnalyticsDemo(day: String, hour: String, total: String) extends Serializable

object AnalyticsDemo  {
  type HBaseRow = java.util.NavigableMap[Array[Byte],
  java.util.NavigableMap[Array[Byte], java.util.NavigableMap[java.lang.Long, Array[Byte]]]]
  // Map(CF -> Map(column qualifier -> Map(timestamp -> value)))
  type CFTimeseriesRow = Map[Array[Byte], Map[Array[Byte], Map[Long, Array[Byte]]]]

  def navMapToMap(navMap: HBaseRow): CFTimeseriesRow =
  navMap.asScala.toMap.map(cf =>
  (cf._1, cf._2.asScala.toMap.map(col =>
  (col._1, col._2.asScala.toMap.map(elem => (elem._1.toLong, elem._2))))))

}
