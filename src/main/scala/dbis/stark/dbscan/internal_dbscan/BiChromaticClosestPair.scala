package dbis.stark.dbscan.internal_dbscan

/**
 * Optimized implementation of the bichromatic closest pair problem for n dimensions.
 * This version uses a K-D Tree for efficient nearest neighbor searches, which is generally
 * faster than the recursive divide-and-conquer approach.
 */
object BiChromaticClosestPair extends BCP {



  /**
   * Find the closest red-blue pair among a set of points using a K-D Tree.
   *
   * @param redPoints  Array of red points.
   * @param bluePoints Array of blue points.
   * @return The closest pair of points (one red, one blue) and their distance.
   */
  def findBichromaticClosestPair(redPoints: Array[Point], bluePoints: Array[Point], bound: Float): ClosestPair = {
    if (redPoints.isEmpty || bluePoints.isEmpty) {
      throw new IllegalArgumentException("Both red and blue point sets must be non-empty")
    }

    // For very small inputs, brute force is faster than building a K-D tree.
    // The threshold can be tuned based on empirical performance testing.
    if (redPoints.length * bluePoints.length <= 200) {
      return bruteForceClosestPair(redPoints, bluePoints, bound)
    }

    // Optimization: Build the K-D tree on the larger point set and query with the smaller one.
    // This minimizes the number of nearest neighbor searches.
    val (pointsToQuery, pointsForTree, queryIsRed) =
      if (redPoints.length < bluePoints.length) {
        (redPoints, bluePoints, true)
      } else {
        (bluePoints, redPoints, false)
      }

    // Build the K-D tree.
    val kdTree = new KDTree(pointsForTree).build()

    var bestQueryPoint: Point = null
    var bestTreePoint: Point = null
    var minDistanceSq = Double.MaxValue

    // Iterate through the smaller set and find the nearest neighbor in the tree.
    for (queryPoint <- pointsToQuery) {
      val (neighbor, distance) = kdTree.nearestNeighbor(queryPoint)
      if (distance <= bound) {
        // If the distance is within the bound, return immediately.
        return if (queryIsRed) {
          ClosestPair(queryPoint, neighbor, distance)
        } else {
          ClosestPair(neighbor, queryPoint, distance)
        }
      }
      val distanceSq = distance * distance // Use squared distance to avoid sqrt in loop
      if (distanceSq < minDistanceSq) {
        minDistanceSq = distanceSq
        bestQueryPoint = queryPoint
        bestTreePoint = neighbor
      }
    }

    // Return the result, ensuring the points are ordered as red (p1) and blue (p2).
    if (queryIsRed) {
      ClosestPair(bestQueryPoint, bestTreePoint, math.sqrt(minDistanceSq))
    } else {
      ClosestPair(bestTreePoint, bestQueryPoint, math.sqrt(minDistanceSq))
    }
  }

  def main(args: Array[String]): Unit = {
    // Create some sample data
    val random = new scala.util.Random(42)
    val dimension = 3

    // Generate 20 random red points
    val redPoints = Array
      .fill(20) {
        Array.fill(dimension)(random.nextFloat() * 10)
      }
      .zipWithIndex
      .map { case (coords, id) =>
        Point(coords, id)
      }

    // Generate 20 random blue points
    val bluePoints = Array
      .fill(20) {
        Array.fill(dimension)(random.nextFloat() * 10)
      }
      .zipWithIndex
      .map { case (coords, id) =>
        Point(coords, id + 20) // Ensure blue points have different IDs
      }

    // Find the closest red-blue pair
    val result = findBichromaticClosestPair(redPoints, bluePoints, 1)

    println(s"Closest red-blue pair:")
    println(s"Red point: ${result.p1.toString}")
    println(s"Blue point: ${result.p2.toString}")
    println(s"Distance: ${result.distance}")
  }

}
