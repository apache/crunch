package crunch.fn

import com.cloudera.crunch.{DoFn, Emitter, Pair => JPair};
import crunch.Conversions

class STableDoTableFn[K, V, L, W](fn: (Any, Any) => Seq[(Any, Any)]) extends DoFn[JPair[K, V], JPair[L, W]] {
  override def process(input: JPair[K, V], emitter: Emitter[JPair[L, W]]): Unit = {
    for ((f, s) <- fn(Conversions.c2s(input.first()), Conversions.c2s(input.second()))) {
      emitter.emit(JPair.of(Conversions.s2c(f), Conversions.s2c(s)).asInstanceOf[JPair[L, W]])
    }
  }
}
