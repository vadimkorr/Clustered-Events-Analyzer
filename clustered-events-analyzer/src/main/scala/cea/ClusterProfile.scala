package cea

import com.vividsolutions.jts.geom.{Coordinate, Geometry, LineString, Point, GeometryFactory}
import com.vividsolutions.jts.algorithm.{ConvexHull}
import com.vividsolutions.jts.io.WKTWriter
import cea._

case class ClusterProfile (
    private val _id: Long,
    private val _objectsCount: Long,
    private var _maxTimeInterval: TimeInterval,
    private var _minTimeInterval: TimeInterval,
    private var _timeRange: TimeInterval,
    private var _boundingHull: Geometry,
    private var _points: Array[Coordinate] = Array.empty[Coordinate]) extends java.io.Serializable {
    
    private def updateMinIntervalIfItGreater(timeInterval: TimeInterval) = {
        println(_minTimeInterval.length + (if (_minTimeInterval.length > timeInterval.length) " > " else " < ") + timeInterval.length)
        if (_minTimeInterval.length > timeInterval.length) _minTimeInterval = timeInterval
    }

    private def updateMaxIntervalIfItSmaller(timeInterval: TimeInterval) = 
        if (_maxTimeInterval.length < timeInterval.length) _maxTimeInterval = timeInterval
    
    private def updateTimeRangeIfItNarrower(timeInterval: TimeInterval) = {
        if (timeInterval.start < _timeRange.start) _timeRange.setStart(timeInterval.start)
        if (timeInterval.end > _timeRange.end) _timeRange.setEnd(timeInterval.end)
    }

    def updateTimeIntervals(timeInterval: TimeInterval) = {
        updateMinIntervalIfItGreater(timeInterval)
        updateMaxIntervalIfItSmaller(timeInterval)
        updateTimeRangeIfItNarrower(timeInterval)
    }

    private def appendCoordinatesFromGeom(ls: LineString) = {
        _points ++= ls.getCoordinates()
    }

    private def appendCoordinatesFromGeom(p: Point) = {
        _points :+= p.getCoordinate()
    }

    def appendCoordinates(geom: Geometry) = {
        geom match {                                 
            case _: LineString => {
                var ls = geom.asInstanceOf[LineString]
                appendCoordinatesFromGeom(ls)
            }
            case _: Point => {
                var p = geom.asInstanceOf[Point]
                appendCoordinatesFromGeom(p)
            }               
        }
    }

    def computeBoundingPolygon() = {
        val geomFactory: GeometryFactory = new GeometryFactory()
        val ch: ConvexHull = new ConvexHull(_points, geomFactory)
        _boundingHull = ch.getConvexHull()
    }

    def getWktFromGeom = (new WKTWriter()).write(_boundingHull)

    override def toString: String = s"${_id};${_objectsCount};${_minTimeInterval};${_maxTimeInterval};${_timeRange};${getWktFromGeom}"
}

object ClusterProfile {
    def apply(id: Long, objectsCount: Long): ClusterProfile = 
        ClusterProfile(id, objectsCount, TimeInterval.apply(Long.MaxValue).setEnd(0), TimeInterval.apply(0).setEnd(Long.MaxValue), TimeInterval.apply(Long.MaxValue).setEnd(0), null)
}