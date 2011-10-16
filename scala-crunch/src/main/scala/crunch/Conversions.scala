package crunch

import com.cloudera.crunch.{Pair => CPair, Tuple3 => CTuple3, Tuple4 => CTuple4}
import com.cloudera.crunch.`type`.{PType, PTypeFamily};

object Conversions {

  def getPType(m: OptManifest[_], ptf: PTypeFamily): PType[_] = {
    println("Manifesting: " + m)
    m match {
      case x: ClassManifest[Traversable[String]] => ptf.strings()
      case x: ClassManifest[Traversable[Long]] => ptf.longs()
      case x: ClassManifest[Traversable[Int]] => ptf.ints()
      case x: ClassManifest[Traversable[Float]] => ptf.floats()
      case x: ClassManifest[Traversable[Double]] => ptf.doubles()
      case x: ClassManifest[Traversable[Boolean]] => ptf.booleans()
      case x: ClassManifest[String] => ptf.strings()
      case x: ClassManifest[Long] => ptf.longs()
      case x: ClassManifest[Int] => ptf.ints()
      case x: ClassManifest[Float] => ptf.floats()
      case x: ClassManifest[Double] => ptf.doubles()
      case x: ClassManifest[Boolean] => ptf.booleans()
      case x: ClassManifest[Tuple2[_, _]] => {
        val ta = x.typeArguments
        ptf.pairs(getPType(ta(0), ptf), getPType(ta(1), ptf))
      }
      case x: ClassManifest[Tuple3[_, _, _]] => {
        val ta = x.typeArguments
        ptf.triples(getPType(ta(0), ptf), getPType(ta(1), ptf), getPType(ta(2), ptf))
      }
      case x: ClassManifest[Tuple4[_, _, _, _]] => {
        val ta = x.typeArguments
        ptf.quads(getPType(ta(0), ptf), getPType(ta(1), ptf), getPType(ta(2), ptf),
          getPType(ta(3), ptf))
      }
      case x: ClassManifest[_] => ptf.records(x.erasure)
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
