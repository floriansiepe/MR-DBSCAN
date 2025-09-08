package dbis.stark.dbscan.internal_dbscan

case class Point(vector: Array[Float], id: Long) {
  var distanceToCenter: Double = _

  def this(id: Long, point: Array[Float]) =
    this(point, id)

  def dim: Int = vector.length

  def apply(index: Int): Float =
    vector(index)

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Point => id == that.id
      case _                 => false
    }

  override def hashCode(): Int =
    id.hashCode()

  override def toString: String =
    s"Point(id=$id, vector=${vector.mkString(", ")}, distanceToCenter=$distanceToCenter)"

}
