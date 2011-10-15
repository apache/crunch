package crunch

import com.cloudera.crunch.{GroupingOptions, PTable => JTable, Pair => JPair}

class PTable[K, V](jtable: JTable[K, V]) extends PCollection[JPair[K, V]](jtable) with JTable[K, V] {

  override def getPTableType() = jtable.getPTableType()

  override def getKeyType() = jtable.getKeyType()

  override def getValueType() = jtable.getValueType()

  override def union(tables: JTable[K, V]*) = {
    new PTable[K, V](jtable.union(tables.map(baseCheck): _*))
  }

  private def baseCheck(c: JTable[K, V]): JTable[K, V] = c match {
    case x: PTable[K, V] => x.base.asInstanceOf[PTable[K, V]]
    case _ => c
  }

  def ++ (other: JTable[K, V]) = union(other)

  override def groupByKey() = new PGroupedTable(jtable.groupByKey())

  override def groupByKey(partitions: Int) = new PGroupedTable(jtable.groupByKey(partitions))

  override def groupByKey(options: GroupingOptions) = new PGroupedTable(jtable.groupByKey(options))
}
