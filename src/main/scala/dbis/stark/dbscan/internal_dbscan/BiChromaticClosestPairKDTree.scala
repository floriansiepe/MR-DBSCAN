package dbis.stark.dbscan.internal_dbscan

object BiChromaticClosestPairKDTree {

  import BiChromaticClosestPair._

  def findBichromaticClosestPair(redPoints: Array[Point], bluePoints: Array[Point], bound: Float): ClosestPair = {
    if (redPoints.length * bluePoints.length <= 100) {
      return bruteForceClosestPair(redPoints, bluePoints, bound)
    }
    val (smallerSet, largerSet, isRed) = if (redPoints.length < bluePoints.length) {
      (redPoints, bluePoints, true)
    } else {
      (bluePoints, redPoints, false)
    }

    val tree                     = new KDTree(smallerSet).build()
    var closestPair: ClosestPair = ClosestPair(null, null, Double.MaxValue)
    for (largerPoint <- largerSet) {
      val (nearestPoint, distance) = tree.nearestNeighbor(largerPoint)
      if (distance <= bound) {
        return ClosestPair(if (isRed) nearestPoint else largerPoint, if (isRed) largerPoint else nearestPoint, distance)
      }
      if (distance < closestPair.distance) {
        closestPair =
          ClosestPair(if (isRed) nearestPoint else largerPoint, if (isRed) largerPoint else nearestPoint, distance)
      }
    }
    closestPair

  }

}
