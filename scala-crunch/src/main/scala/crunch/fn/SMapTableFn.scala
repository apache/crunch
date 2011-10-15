package crunch.fn

import com.cloudera.crunch.{MapFn, Pair => JPair};
import crunch.Conversions

class SMapTableFn[S, K, V](fn: Any => (K, V)) extends MapFn[S, JPair[K, V]] {
  override def map(input: S): JPair[K, V] = {
    Conversions.s2c(fn(Conversions.c2s(input))).asInstanceOf[JPair[K, V]]
  }
}
