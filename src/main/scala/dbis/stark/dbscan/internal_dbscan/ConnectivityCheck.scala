package dbis.stark.dbscan.internal_dbscan

trait ConnectivityCheck {

  def isConnected(g: RangeQuery with TreeRoot, h: RangeQuery with TreeRoot, coreFlags: collection.Map[Int, Boolean], epsilon: Float, radius: Float): Boolean

}

sealed class BichromaticClosestPairConnectivityCheck extends ConnectivityCheck {

  override def isConnected(
                            g: RangeQuery with TreeRoot,
                            h: RangeQuery with TreeRoot,
                            coreFlags: collection.Map[Int, Boolean],
                            epsilon: Float,
                            radius: Float
  ): Boolean = {
    // Find all relevant points in the cells g and h by performing a range query
    // around the centers of g and h with a radius of 1.5 * epsilon because the max distance
    // between a cell center and a point in the cell is at most 0.5 * epsilon. Then we need to
    // add epsilon to the distance between the centers of g and h to ensure that we find all relevant points.
    val relevantPointsG = g.rangeQuery(h.root, radius) // Adding a small epsilon to handle floating-point precision issues
    val relevantPointsH = h.rangeQuery(g.root, radius) // Adding a small epsilon to handle floating-point precision issues
    val corePointsG     = relevantPointsG.filter(p => coreFlags.getOrElse(p.id.toInt, false))
    val corePointsH     = relevantPointsH.filter(p => coreFlags.getOrElse(p.id.toInt, false))
    if (corePointsG.isEmpty || corePointsH.isEmpty) return false
    val closestPair =
      BiChromaticClosestPairKDTree.findBichromaticClosestPair(corePointsG, corePointsH, epsilon)
    // Explicitly access the distance field from the ClosestPair case class
    closestPair.distance <= epsilon// Adding a small epsilon to handle floating-point precision issues
  }

}
