package dbis.stark.dbscan

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

import scala.util.Using
//import org.apache.log4j.Logger

case class DBSCANConfig(input: java.net.URI = new java.net.URI("."),
                        output: java.net.URI = new java.net.URI("."),
                        eps: Double = 0.1,
                        minPts: Int = 10,
                        ppd: Int = 0,
                        maxPSize: Int = 100,
                        numDimensions: Int = -1)

/**
 * Start with:
 * spark-submit --class dbis.dbscan.Main --master local[4] \
 * target/scala-2.11/dbscan-0.1.jar \
 * --input data/labeled_data.csv --output test-res --eps 0.3 --minPts 10 --ndims 2
 *
 */
object Main {
  def main(args: Array[String]) {

    var inputFile: java.net.URI = null
    var outputFile: java.net.URI = null
    var eps: Double = 0.1
    var minPts: Int = 10
    var ppd: Int = 0
    var maxPartitionSize: Int = 100
    var numDimensions: Int = -1

    //    val log = Logger.getLogger(getClass.getName)

    val parser = new scopt.OptionParser[DBSCANConfig]("DBSCAN") {
      head("DBSCAN", "0.1")
      opt[java.net.URI]('i', "input") action { (x, c) => c.copy(input = x) } text ("input is the input file")
      opt[java.net.URI]('o', "output").required() action { (x, c) => c.copy(output = x) } text ("output is the result file")
      opt[Double]("eps") action { (x, c) => c.copy(eps = x) } text ("epsilon parameter for DBSCAN")
      opt[Int]("minPts") action { (x, c) => c.copy(minPts = x) } text ("minPts parameter for DBSCAN")
      opt[Int]("ppd") action { (x, c) => c.copy(ppd = x) } text ("partitions per dimension (enforces grid partitioning)")
      opt[Int]('s', "maxPartitionSize") action { (x, c) => c.copy(maxPSize = x) } text ("maximum partition size (enforces binary space partitioning)")
      opt[Int]('n', "ndims") action { (x, c) => c.copy(numDimensions = x) } text ("number of dimensions (fields considered for clustering, default = all fields)")
      help("help") text ("prints this usage text")
    }

    // parser.parse returns Option[C]
    parser.parse(args, DBSCANConfig()) match {
      case Some(config) => {
        // do stuff
        inputFile = config.input
        outputFile = config.output
        eps = config.eps
        minPts = config.minPts
        ppd = config.ppd
        maxPartitionSize = config.maxPSize
        numDimensions = config.numDimensions
      }
      case None =>
        // arguments are bad, error message will have been displayed
        return
    }

    val conf = new SparkConf().setAppName("DBSCAN")
    val sc = new SparkContext(conf)

    val msg = if (numDimensions == -1) {
      numDimensions = 20; "all"
    } else numDimensions
    //    log.info(s"starting DBSCAN with eps=$eps, minPts=$minPts on $msg dimensions of file '${inputFile.toString}'")

    val dbscan = new DBScan[Long, Int]().setEpsilon(eps).setMinPts(minPts)

    if (ppd > 0) dbscan.setPPD(ppd) else dbscan.setMaxPartitionSize(maxPartitionSize)

    val data = sc.textFile(inputFile.toString(), 3 * sc.defaultParallelism)
      // Skip header line
      .filter(line => !line.startsWith("dim"))
      .map(line => line.split(",").slice(0, numDimensions).map(_.toDouble))
      .zipWithIndex // to create a unique key for each entry - this triggers a spark job if there is more than one partition
      .map { case (t, i) => (i, Vectors.dense(t), 1) }

    val (model, timings) = dbscan.run(data)
    val outputPath = new Path(outputFile.toString())
    val fs = FileSystem.get(new Configuration())
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
    val timingsPath = new Path(outputPath.getParent, outputPath.getName + "timings.csv")
    if (fs.exists(timingsPath)) {
      fs.delete(timingsPath, true)
    }
    val header = timings.keys.mkString(",") + "\n"
    val values = timings.values.mkString(",") + "\n"
    Using(fs.create(timingsPath)) { os =>
      os.write(header.getBytes("UTF-8"))
      os.write(values.getBytes("UTF-8"))
    }

    model.points.coalesce(1).saveAsTextFile(outputFile.toString())

    sc.stop()
  }
}
