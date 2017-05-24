package cea

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import com.vividsolutions.jts.geom._
import cea._

case class CEAConfig(input: java.net.URI = new java.net.URI("."),
                     output: java.net.URI = new java.net.URI("."))

/**
  * spark-submit --class cean.Main --master local[4] \
  *        target/scala-2.11/cea.jar \
  *        --input input/clusters.csv --output results
  */

object Main {

  def processCluster(iter: (Long, Iterable[STObject])): ClusterProfile = {
      val it = iter._2
      var cp: ClusterProfile = ClusterProfile.apply(it.head.getClusterId, it.size, it.head.getTimeInterval)
      
      it.foreach((stObj) => {
        cp.updateTimeIntervals(stObj.getTimeInterval)
        cp.appendCoordinates(stObj.getGeom)
      })
      cp.computeBoundingPolygon()
      cp
  }

  def run(input: RDD[STObject]): ClusterProfilesModel = {
      val clusterSets = input.groupBy(k => k.getClusterId)
      val clusterResults = clusterSets.map(stObj => processCluster(stObj))
      new ClusterProfilesModel(clusterResults)
  }

  def main(args: Array[String]) {
    var inputFile: java.net.URI = null
    var outputFile: java.net.URI = null

    val parser = new scopt.OptionParser[CEAConfig]("CEA") {
      head("CEA", "0.1")
      opt[java.net.URI]('i', "input")
        .action { (x, c) => c.copy(input = x) }
        .text("input is the input file")
      opt[java.net.URI]('o', "output")
        .required()
        .action { (x, c) => c.copy(output = x) }
        .text("output is the result file")
      help("help")
        .text ("prints this usage text")
    }

    parser.parse(args, CEAConfig()) match {
      case Some(config) => {
        inputFile = config.input
        outputFile = config.output
      }
      case None =>
        return
    }

    val conf = new SparkConf().setAppName("CEA")
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputFile.toString(), 3 * sc.defaultParallelism)
        .map(line => { 
          var inputArr = line.split(";")
          var wkt: String = inputArr(1)
          var radius: Double = inputArr(2).toDouble
          var start: String = inputArr(3)
          var end: String = inputArr(4)
          var clusterId: Long = inputArr(5).toLong
          var timeInterval: TimeInterval = (if(end.isEmpty) TimeInterval.apply(start.toLong) else TimeInterval.apply(start.toLong, end.toLong))
          var t: STObject = STObject.apply(wkt, timeInterval, radius, clusterId)
          t
        })
    val model = run(data)
    model.getProfiles.coalesce(1).saveAsTextFile(outputFile.toString())

    sc.stop()
  }
}