package crunch

import com.cloudera.crunch.{DoFn, Emitter, Pair => JPair};

class STableDoFn[K, V, T](fn: (Any, Any) => Seq[T]) extends DoFn[JPair[K, V], T] {
  override def process(input: JPair[K, V], emitter: Emitter[T]): Unit = {
    for (val v <- fn(Conversions.c2s(input.first()), Conversions.c2s(input.second()))) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[T])
    }
  }
}
