package cea

import com.vividsolutions.jts.geom.{Geometry, Point, LineString}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import STObject._
import cea._
import com.vividsolutions.jts.io.WKTReader

case class STObject(
   private val _geom: Geometry,
   private val _timeInterval: TimeInterval,
   private val _radius: Double,
   private val _clusterId: Long) extends java.io.Serializable {
  
  def getGeom = _geom
  def getTimeInterval = _timeInterval
  def getRadius = _radius
  def getClusterId = _clusterId
}

object STObject {
  def apply(wkt: String, start: Long, end: Long, radius: Double, clusterId: Long): STObject = this(new WKTReader().read(wkt), new TimeInterval(start, end), radius, clusterId)
  def apply(wkt: String, timeInterval: TimeInterval, radius: Double, clusterId: Long): STObject = this(new WKTReader().read(wkt), timeInterval, radius, clusterId) 
}