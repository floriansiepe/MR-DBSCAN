package dbis.stark.dbscan.internal_dbscan

trait UnionFind[T] {

  def union(v1: T, v2: T): Unit

  def find(v: T): T

  def getComponents: collection.Set[collection.Set[T]]

}
