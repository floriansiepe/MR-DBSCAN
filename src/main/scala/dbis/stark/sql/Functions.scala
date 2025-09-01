package dbis.stark.sql

import dbis.stark.STObject
import dbis.stark.sql.raster._
import dbis.stark.sql.spatial._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Functions {

  spatial.registerUDTs()
  raster.registerUDTs()

  val fromWKT = udf(STObject.fromWKT _)

  def register(implicit spark: SparkSession): Unit = {


    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_wkt", STAsWKT, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_geomfromwkt", STGeomFromWKT, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_geomfromtile", STGeomFromTile, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_point", STPoint, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_sto", MakeSTObject, "builtin")


    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_contains", STContains, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_containedby", STContainedBy, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_intersects", STIntersects, "builtin")

    //Select-Getters
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ulx", GetUlx, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("uly", GetUly, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("width", GetWidth, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("height", GetHeight, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("data", GetData, "builtin")

    spark.sessionState.functionRegistry.createOrReplaceTempFunction("r_max", TileMax, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("r_min", TileMin, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("r_median", TileMedian, "builtin")


    // Histogram functions
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("histogram", CalcTileHistogram, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("h_value", HistogramValue, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("h_lower", HistogramLowerBounds, "builtin")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("h_upper", HistogramUpperBounds, "builtin")
  }
}
