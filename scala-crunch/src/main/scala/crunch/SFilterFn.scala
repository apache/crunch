package crunch

import com.cloudera.crunch.FilterFn;

class SFilterFn[T](f: Any => Boolean) extends FilterFn[T] {
  override def accept(input: T): Boolean = {
    f(Conversions.c2s(input));
  }
}
