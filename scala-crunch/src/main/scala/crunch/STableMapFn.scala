package crunch

import com.cloudera.crunch.{MapFn, Pair => JPair};

class STableMapFn[K, V, T](fn: (Any, Any) => T) extends MapFn[JPair[K, V], T] {
  override def map(input: JPair[K, V]): T = {
    val v = fn(Conversions.c2s(input.first()), Conversions.c2s(input.second()))
    Conversions.s2c(v).asInstanceOf[T]
  }
}
