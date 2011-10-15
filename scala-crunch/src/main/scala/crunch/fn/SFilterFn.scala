package crunch.fn

import com.cloudera.crunch.FilterFn;
import crunch.Conversions

class SFilterFn[T](f: Any => Boolean) extends FilterFn[T] {
  override def accept(input: T): Boolean = {
    f(Conversions.c2s(input));
  }
}
