package crunch

import com.cloudera.crunch.{DoFn, Emitter};

class SDoFn[S, T](fn: Any => Seq[T]) extends DoFn[S, T] {
  override def process(input: S, emitter: Emitter[T]): Unit = {
    for (val v <- fn(Conversions.c2s(input))) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[T])
    }
  }
}
