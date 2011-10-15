package crunch

import com.cloudera.crunch.{MapFn, Pair => JPair};

class STableMapTableFn[K, V, L, W](fn: (Any, Any) => (Any, Any)) extends MapFn[JPair[K, V], JPair[L, W]] {
  override def map(input: JPair[K, V]): JPair[L, W] = {
    val (f, s) = fn(Conversions.c2s(input.first()), Conversions.c2s(input.second()))
    JPair.of(Conversions.s2c(f), Conversions.s2c(s)).asInstanceOf[JPair[L, W]]
  }
}
