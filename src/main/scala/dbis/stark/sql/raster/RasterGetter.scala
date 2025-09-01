package dbis.stark.sql.raster

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.raster.TileUDT
import org.apache.spark.sql.types._

abstract class RasterGetter(c: Expression) extends Expression with CodegenFallback{

  def child: Expression = c

  override def children = Seq(child)

  override def nullable = false
}

case class GetUlx(exprs: Seq[Expression]) extends RasterGetter(exprs.head) {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow): Double = {
    val tile = TileUDT.deserialize(child.eval(input))
    tile.ulx
  }

  override def dataType: DataType = DoubleType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(exprs = newChildren)
}

case class GetUly(exprs: Seq[Expression]) extends RasterGetter(exprs.head) {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow): Double = {
    val tile = TileUDT.deserialize(child.eval(input))
    tile.uly
  }

  override def dataType: DataType = DoubleType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(exprs = newChildren)
}

case class GetWidth(exprs: Seq[Expression]) extends RasterGetter(exprs.head) {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow): Int = {
    val tile = TileUDT.deserialize(child.eval(input))
    tile.width
  }

  override def dataType: DataType = IntegerType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(exprs = newChildren)
}

case class GetHeight(exprs: Seq[Expression]) extends RasterGetter(exprs.head) {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow): Int = {
    val tile = TileUDT.deserialize(child.eval(input))
    tile.height
  }

  override def dataType: DataType = IntegerType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(exprs = newChildren)
}

case class GetData(exprs: Seq[Expression]) extends RasterGetter(exprs.head) {
  require(exprs.length == 1, s"Exactly one expression allowed for ${this.getClass.getSimpleName}, but got ${exprs.length}")

  override def eval(input: InternalRow): GenericArrayData = {
    val tile = TileUDT.deserialize(child.eval(input))
    new GenericArrayData(tile.data)
  }

  override def dataType: DataType = ArrayType(ByteType)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(exprs = newChildren)
}
