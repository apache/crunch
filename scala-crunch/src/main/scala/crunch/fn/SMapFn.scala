package crunch.fn

import com.cloudera.crunch.MapFn;
import crunch.Conversions

class SMapFn[S, T](fn: Any => T) extends MapFn[S, T] {
  override def map(input: S): T = {
    Conversions.s2c(fn(Conversions.c2s(input))).asInstanceOf[T]
  }
}
