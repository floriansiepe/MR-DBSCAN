package dbis.stark.dbscan.internal_dbscan

import dbis.stark.dbscan.internal_dbscan.Distances.euclideanDistanceSquared

import scala.collection.mutable
import scala.language.postfixOps
import scala.util.control.Breaks.{break, breakable}

class DBSCANGrid(
    val epsilon: Float,
    val minPts: Int,
    val connectivityCheck: ConnectivityCheck = new BichromaticClosestPairConnectivityCheck()
) extends BaseDBSCAN {

  private val epsilonSquared = epsilon * epsilon

  private var numClusters: Int = _

  // Use maps to store core and border points per cluster, similar to your classic implementation
  private val corePointsMap: mutable.Map[Int, mutable.Set[Point]] = mutable.Map.empty

  private val borderPointsMap: mutable.Map[Int, mutable.Set[Point]] = mutable.Map.empty

  @inline override def fit(points: Array[Point]): (Array[Int], Array[Point]) = {
    if (points.isEmpty) {
      throw new IllegalArgumentException("Points collection must not be empty")
    }
    // Clear maps for consecutive runs
    corePointsMap.clear()
    borderPointsMap.clear()

    val grid                   = makeCells(points)
    val (coreCells, coreFlags) = markCore(grid)
    val clusters               = clusterCore(grid, coreCells, coreFlags)
    clusterBorder(grid, coreFlags, clusters)

    val labels = points.map(p => clusters.getOrElse(p.id.toInt, -1))
    (labels, points)
  }

  /** Identifies border points and assigns them to a cluster. A border point is a non-core point that is within the
    * epsilon radius of a core point.
    */
  @inline  private def clusterBorder(
      grid: Grid,
      coreFlags: mutable.Map[Int, Boolean],
      clusters: mutable.Map[Int, Int]
  ): Unit = {
    // Iterate over ALL cells, not just those with less than minPts.
    // A non-core point (potential border point) can exist in any cell.
    for (cell <- grid.cellsMap.values.filter(_.numPoints < minPts)) {
      // Find all points in the cell that were NOT marked as core points.
      val potentialBorderPoints = cell.points.filter(p => !coreFlags.getOrElse(p.id.toInt, false))

      for (point <- potentialBorderPoints) {
        // Query the current cell and its neighbors.
        val cellsToSearch = Vector(cell) ++ grid.queryEpsNeighborCells(cell.id)
        breakable {
          for (neighborCell <- cellsToSearch) {
            // Find a core point in a neighboring cell that is within epsilon distance.
            for (coreNeighbor <- neighborCell.points.filter(p => coreFlags.getOrElse(p.id.toInt, false))) {
              if (euclideanDistanceSquared(point.vector, coreNeighbor.vector) <= epsilonSquared) {
                val clusterId = clusters.getOrElse(coreNeighbor.id.toInt, -1)
                if (clusterId != -1) {
                  clusters(point.id.toInt) = clusterId
                  // Add the point to the border points map for that cluster.
                  borderPointsMap.getOrElseUpdate(clusterId, mutable.Set.empty) += point
                  // Once assigned, break to avoid re-assigning to another cluster.
                  break()
                }
              }
            }
          }
        }
      }
    }
  }

  @inline  private def makeCells(points: Array[Point]): Grid = {
    val dim = points.head.vector.length
    new Grid(points, epsilon, dim)
  }

  @inline private def markCore(grid: Grid): (Array[GridCell], mutable.Map[Int, Boolean]) = {
    val coreFlags = mutable.Map.empty[Int, Boolean]
    val coreCells = mutable.Set[GridCell]()

    for (cell <- grid.cellsMap.values) {
      // Optimization: If a cell has >= minPts, assume all points in it are core.
      // This is a primary source of difference with classic DBSCAN.
      if (cell.numPoints >= minPts) {
        cell.setIsCore(true)
        coreCells += cell
        for (point <- cell.points)
          coreFlags(point.id.toInt) = true
      } else {
        // For less dense cells, check each point individually.
        for (point <- cell.points) {
          breakable {
            var count         = cell.rangeCount(point, grid.epsilon)
            val neighborCells = grid.queryEpsNeighborCells(cell.id)
            for (neighbor <- neighborCells) {
              count += neighbor.rangeCount(point, grid.epsilon)
              if (count >= minPts) {
                coreFlags(point.id.toInt) = true
                // The cell containing a core point is considered a core cell for clustering.
                if (!cell.isCore) {
                  cell.setIsCore(true)
                  coreCells += cell
                }
                break()
              }
            }
          }
        }
      }
    }

    (coreCells.toArray, coreFlags)
  }

  @inline private def clusterCore(
      grid: Grid,
      coreCells: Array[GridCell],
      coreFlags: mutable.Map[Int, Boolean]
  ): mutable.Map[Int, Int] = {
    val radius          = epsilon * math.sqrt(grid.dim + 3).toFloat * 1.0000001f
    val uf              = new StaticUnionFind[GridCell](coreCells.toSet)
    val sortedCoreCells = coreCells.sortBy(_.numPoints)(Ordering[Int].reverse)
    for (cell <- sortedCoreCells) {
      for (neighbor <- grid.queryEpsNeighborCells(cell.id).filter(_.isCore)) {
        if (cell.id > neighbor.id && uf.find(cell) != uf.find(neighbor)) {
          if (connectivityCheck.isConnected(cell, neighbor, coreFlags, epsilon, radius)) {
            uf.union(cell, neighbor)
          }
        }
      }
    }

    val clusters        = mutable.Map.empty[Int, Int]
    var clusterID       = 0
    val rootToClusterID = mutable.Map.empty[GridCell, Int]

    for (cell <- sortedCoreCells) {
      val root = uf.find(cell)
      if (!rootToClusterID.contains(root)) {
        clusterID += 1
        rootToClusterID(root) = clusterID
      }
      val currentClusterId = rootToClusterID(root)
      for (point <- cell.points.filter(p => coreFlags.getOrElse(p.id.toInt, false))) {
        clusters(point.id.toInt) = currentClusterId
        corePointsMap.getOrElseUpdate(currentClusterId, mutable.Set.empty) += point
      }
    }
    numClusters = uf.numComponents
    clusters
  }

  @inline override def getPointsArray(pointsMap: mutable.Map[Int, mutable.Set[Point]]): Array[Array[Point]] =
    pointsMap.toArray.sortBy(_._1).map(_._2.toArray)

  override def getNumClusters: Int = numClusters

  override def getCorePoints: Array[Array[Point]] = getPointsArray(corePointsMap)

  override def getBorderPoints: Array[Array[Point]] = getPointsArray(borderPointsMap)

}

case class GridCell(
    id: Long,
    gridCoords: Seq[Int],
    length: Float,
    grid: Grid,
    searchRadius: Float,
    points: mutable.ArrayBuffer[Point] = mutable.ArrayBuffer.empty
) extends RangeQuery
    with TreeRoot {

  def root: Array[Float] = center

  val center: Array[Float] =  gridCoords.map(c => (c * length + 0.5 * length).toFloat).toArray // Center of the cell in integer coordinates

  // Lazy initialization of the tree is important, otherwise the tree is incomplete
  private lazy val tree = buildTree()

  var isCore: Boolean = false

  def numPoints: Int = points.length

  def setIsCore(isCore: Boolean): Unit =
    this.isCore = isCore

  override def hashCode(): Int = id.toInt

  override def equals(obj: Any): Boolean = obj match {
    case that: GridCell => this.id == that.id
    case _              => false
  }

  private def buildTree() = {
    // implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    val indexTree = new QuadTree(points).build()
    indexTree
  }

  override def rangeQuery(anchorPoint: Array[Float], range: Float): Array[Point] =
    tree.rangeQuery(anchorPoint, range)

}

class Grid(val points: Array[Point], val epsilon: Float, val dim: Int) {

  val cellsMap: mutable.Map[Vector[Int], GridCell] = mutable.Map.empty

  val cellIdToKey: mutable.Map[Long, Vector[Int]] = mutable.Map.empty

  // Ensure dependencies are initialized before building the center tree
  private val cellSize: Float = epsilon / math.sqrt(dim).toFloat

  // See https://github.com/wangyiqiu/dbscan-python/blob/42e1e719c2aa363c8991a0ada8e52a32b8702b15/include/dbscan/grid.h#L169
  // analytically, the search radius should be 2 * epsilon, but the reference implementation uses this value
  // cellSize * math.sqrt(dim + 3).toFloat * 1.0000001f
  private val searchRadius: Float = math.sqrt(dim + 3).toFloat * 1.0000001f * epsilon

  private val minRealCoords: Array[Float] = Array.fill(dim)(0f)

  private val gridOrigin = minRealCoords.map(c => c - 0.5f * cellSize)

  private val dimRange = (0 until dim).toArray

  val centerTree: KDTree = initializeGrid()

  private val neighborCache = mutable.Map.empty[Long, Vector[GridCell]]

  @inline def queryEpsNeighborCells(cellIdx: Long): Vector[GridCell] = {
    if (neighborCache.contains(cellIdx)) {
      return neighborCache(cellIdx)
    }
    val cellCoords = cellIdToKey(cellIdx)
    val cell       = cellsMap(cellCoords)
    val neighbors  = centerTree.rangeQuery(cell.center, searchRadius).filter(_.id != cell.id)
    val neighborCells = neighbors.toVector.map { neighbor =>
      val neighborCellCoords = cellIdToKey(neighbor.id)
      cellsMap(neighborCellCoords)
    }
    neighborCache(cellIdx) = neighborCells
    neighborCells
  }

  @inline private def initializeGrid(): KDTree = {
    if (points.isEmpty) {
      throw new IllegalArgumentException("Points collection must not be empty")
    }

    var cellId = 0
    for (point <- points) {
      val cellCoords = getIntegerGridCoords(point)
      if (cellsMap.contains(cellCoords)) {
        cellsMap(cellCoords).points += point
      } else {
        val cell = GridCell(cellId, cellCoords, cellSize, this, searchRadius, mutable.ArrayBuffer(point))
        cellsMap(cellCoords) = cell
        cellIdToKey(cellId) = cellCoords
        cellId += 1
      }
    }

    val cellCenters = cellsMap.values.map(cell => Point(cell.center, cell.id))
    new KDTree(cellCenters.toArray).build()

  }

  @inline  private def getIntegerGridCoords(point: Point) =
    dimRange.map { d =>
      math.floor((point.vector(d) - gridOrigin(d)) / cellSize).toInt
    }.toVector

}
