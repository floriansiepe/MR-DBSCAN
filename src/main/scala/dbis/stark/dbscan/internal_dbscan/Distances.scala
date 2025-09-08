package dbis.stark.dbscan.internal_dbscan

object Distances {
  def euclideanDistanceSquared(p1: Array[Float], p2: Array[Float]): Float = {
    var sum = 0.0f
    for (i <- p1.indices) {
      val diff = p1(i) - p2(i)
      sum += diff * diff
    }
    sum
  }

  def euclideanDistance(p1: Array[Float], p2: Array[Float]): Float = {
    math.sqrt(euclideanDistanceSquared(p1, p2)).toFloat
  }

}
