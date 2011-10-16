package crunch

import com.cloudera.crunch.{Pair => CPair, Tuple3 => CTuple3, Tuple4 => CTuple4}
import com.cloudera.crunch.`type`.{PType, PTypeFamily};

object Conversions {

  def getPType[S](m: ClassManifest[S], ptf: PTypeFamily): PType[_] = {
    val clazz = m.erasure
    if (classOf[java.lang.String].equals(clazz)) {
      ptf.strings()
    } else if (classOf[Double].equals(clazz)) {
      ptf.doubles()
    } else if (classOf[Boolean].equals(clazz)) {
      ptf.booleans()
    } else if (classOf[Float].equals(clazz)) {
      ptf.floats()
    } else if (classOf[Int].equals(clazz)) {
      ptf.ints()
    } else if (classOf[Long].equals(clazz)) {
      ptf.longs()
    } else if (classOf[Traversable[_]].equals(clazz)) {
      getPType(m.typeArguments(0).asInstanceOf[ClassManifest[_]], ptf)
    } else if (classOf[Tuple2[_, _]].equals(clazz)) {
      val ta = m.typeArguments.map(_.asInstanceOf[ClassManifest[_]])
      ptf.pairs(getPType(ta(0), ptf), getPType(ta(1), ptf))
    } else if (classOf[Tuple3[_, _, _]].equals(clazz)) {
      val ta = m.typeArguments.map(_.asInstanceOf[ClassManifest[_]])
      ptf.triples(getPType(ta(0), ptf), getPType(ta(1), ptf), getPType(ta(2), ptf))
    } else if (classOf[Tuple4[_, _, _, _]].equals(clazz)) {
      val ta = m.typeArguments.map(_.asInstanceOf[ClassManifest[_]])
      ptf.quads(getPType(ta(0), ptf), getPType(ta(1), ptf), getPType(ta(2), ptf),
          getPType(ta(3), ptf))
    } else {
      println("Could not match manifest: " + m + " with class: " + clazz)
      ptf.records(clazz)
    }
  }

  def s2c(obj: Any): Any = obj match {
    case x: Tuple2[_, _] =>  CPair.of(s2c(x._1), s2c(x._2))
    case x: Tuple3[_, _, _] => new CTuple3(s2c(x._1), s2c(x._2), s2c(x._3))
    case x: Tuple4[_, _, _, _] => new CTuple4(s2c(x._1), s2c(x._2), s2c(x._3), s2c(x._4))
    case _ => obj
  }

  def c2s(obj: Any): Any = obj match {
    case x: CPair[_, _] => (c2s(x.first()), c2s(x.second()))
    case x: CTuple3[_, _, _] => (c2s(x.first()), c2s(x.second()), c2s(x.third()))
    case x: CTuple4[_, _, _, _] => (c2s(x.first()), c2s(x.second()), c2s(x.third()),
        c2s(x.fourth()))
    case _ => obj
  }
}
