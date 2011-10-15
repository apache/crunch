package crunch.fn

import com.cloudera.crunch.{DoFn, Emitter, Pair => JPair};
import crunch.Conversions

class SDoTableFn[S, K, V](fn: Any => Seq[(K, V)]) extends DoFn[S, JPair[K, V]] {
  override def process(input: S, emitter: Emitter[JPair[K, V]]): Unit = {
    for (v <- fn(Conversions.c2s(input))) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[JPair[K, V]])
    }
  }
}
