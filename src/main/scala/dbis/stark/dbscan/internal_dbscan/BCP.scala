package dbis.stark.dbscan.internal_dbscan

case class ClosestPair(p1: Point, p2: Point, distance: Double)

trait BCP {
  /** Compute the Euclidean distance between two points */
  def distance(p1: Point, p2: Point): Float =
    Distances.euclideanDistance(p1.vector, p2.vector)

  def findBichromaticClosestPair(redPoints: Array[Point], bluePoints: Array[Point], bound: Float): ClosestPair

  /** Brute force approach for finding the closest pair */
  def bruteForceClosestPair(redPoints: Array[Point], bluePoints: Array[Point], bound: Float): ClosestPair = {
    var minDistance             = Double.MaxValue
    var closestRedPoint: Point  = null
    var closestBluePoint: Point = null

    for {
      redPoint  <- redPoints
      bluePoint <- bluePoints
    } {
      val dist = distance(redPoint, bluePoint)
      if (dist < bound) {
        return ClosestPair(redPoint, bluePoint, dist) // Return immediately if a pair is found within the bound
      }
      if (dist < minDistance) {
        minDistance = dist
        closestRedPoint = redPoint
        closestBluePoint = bluePoint
      }
    }

    ClosestPair(closestRedPoint, closestBluePoint, minDistance)
  }
}
