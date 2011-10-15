package crunch

import com.cloudera.crunch.{CombineFn, PGroupedTable => JGroupedTable, PTable => JTable, Pair => JPair}
import java.lang.{Iterable => JIterable}

class PGroupedTable[K, V](grouped: JGroupedTable[K, V]) extends PCollection[JPair[K, JIterable[V]]](grouped) with JGroupedTable[K, V] {
  def combineValues(fn: CombineFn[K, V]) = new PTable[K, V](grouped.combineValues(fn))

  def ungroup() = new PTable[K, V](grouped.ungroup())
}
